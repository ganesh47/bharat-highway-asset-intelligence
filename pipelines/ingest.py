from __future__ import annotations

import argparse
import copy
import os
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

import pandas as pd

from research.loader import load_inventory
from pipelines.connectors import CONNECTORS
from pipelines.common import ensure_dirs, write_catalog, write_json, read_json, write_parquet, sha256_for_file, dataframe_checksum
from pipelines.quality import evaluate, evidence_status, semantic_errors, observation_date, DOCUMENT_SOURCES

NHAI_EXTRACTION_QUALITY_SOURCE_IDS = {"nhai_annual_report_documents"}
SUCCESS_STATUSES = {"ok", "automated", "manual_ingest", "validated", "generated"}
REFRESH_OUTCOMES = {
    "updated", "checked_unchanged", "retained_after_failure", "manual_evidence_required",
    "restricted", "metadata_only", "model_generated", "unavailable", "not_mapped",
}
LEGACY_SCOPE_METADATA = {
    "data_gov_in_nhai_project_finance_api": {"agency": "MoRTH", "entity_type": "programme", "entity_id": "nh-development-maintenance-pm-gatishakti", "road_class": "National Highway", "scope_note": "NH development and maintenance funding; not NHAI standalone accounts. FY2024-25 is through 31 January 2025."},
    "data_gov_in_nh_fatalities_injuries_state_year": {"agency": "MoRTH", "entity_type": "state_ut", "road_class": "National Highway including expressways"},
    "data_gov_in_nhai_stateut_project_delay_status_2024": {"agency": "MoRTH", "entity_type": "state_ut", "road_class": "National Highway", "source_as_of_date": "2024-03-31"},
    "nhai_constructed_length_series_official": {"agency": "NHAI", "entity_type": "agency", "entity_id": "nhai", "road_class": "National Highway", "scope_note": "NHAI-only annual construction flow; not national network stock."},
    "morth_annual_report_pdf": {"agency": "MoRTH", "entity_type": "state_ut", "road_class": "Mixed NH and State Roads programmes", "scope_note": "NH network stock and CRIF allocations are separate metrics; CRIF state roads are not exclusively State Highways."},
    "parliament_qa_nh_blackspots_state": {"agency": "MoRTH", "entity_type": "state_ut", "road_class": "National Highway"},
}


def find_connector_for_source(source_id: str):
    return next((connector for connector in CONNECTORS if source_id in connector.spec.source_ids), None)


def _load_nhai_extraction_quality(processed_root: Path, output_table_path: str | None) -> dict | None:
    quality_path = processed_root / "nhai_annual_report_tables" / "quality_report.json"
    manifest_path = processed_root / "nhai_annual_report_tables" / "extraction_manifest.json"
    if not quality_path.exists() or not manifest_path.exists() or not output_table_path:
        return None
    quality_payload, manifest_payload = read_json(quality_path), read_json(manifest_path)
    expected = Path(output_table_path)
    input_sha = manifest_payload.get("source_parquet_sha256")
    # A matching pathname is insufficient: re-ingestion can replace the document list.
    if (not expected.is_file() or not input_sha
            or input_sha != quality_payload.get("source_parquet_sha256")
            or input_sha != sha256_for_file(expected)):
        return None
    total = quality_payload.get("canonical_rows", quality_payload.get("total_rows"))
    if total != manifest_payload.get("rows_merged"):
        return None
    canonical = processed_root / "nhai_annual_report_tables_canonical.parquet"
    if not canonical.exists():
        return None
    canonical_sha = manifest_payload.get("canonical", {}).get("sha256")
    if canonical_sha and canonical_sha != sha256_for_file(canonical):
        return None
    if len(pd.read_parquet(canonical)) != total:
        return None
    for field in ("quality", "method_mix"):
        if not isinstance(quality_payload.get(field), dict):
            return None
    return {
        "generated_at": quality_payload.get("generated_at"),
        "quality": quality_payload["quality"], "method_mix": quality_payload["method_mix"],
        "parser_environment": quality_payload.get("parser_environment", {}),
        "source_parquet": str(expected), "source_parquet_sha256": input_sha,
        "quality_report_path": str(quality_path), "extraction_manifest_path": str(manifest_path),
        "rows_merged": total, "document_refresh_outcomes": manifest_payload.get("document_refresh_outcomes", []),
    }


def _base_manifest(source: dict, output_path: Path) -> dict:
    category = source.get("metric_category") or ("model_output" if source.get("retrieval_method") == "model_generation" else "official_measured")
    return {
        "source_id": source["source_id"], "status": "failed", "metric_category": category,
        "source": {"publisher": source.get("publisher_org", "Unknown publisher"), "title": source.get("dataset_title"),
                   "url": source.get("resource_page_url") or source.get("url"),
                   "official_flag": source.get("official_flag", False), "license_terms": source.get("license_terms", "Unknown")},
        "citations": {"permanent_identifier": source.get("permanent_identifier_hint") or source["source_id"],
                      "anchor": "unavailable_no_observations", "note": "No validated observations published."},
        "manifest": {"raw_files": [], "output_files": [], "row_count": 0, "columns": []},
        "output_table_path": str(output_path),
    }


def _read_frame(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


def _rewrite_paths(value, staged: Path, published: Path):
    if isinstance(value, dict):
        return {k: _rewrite_paths(v, staged, published) for k, v in value.items()}
    if isinstance(value, list):
        return [_rewrite_paths(v, staged, published) for v in value]
    if isinstance(value, str) and value.startswith(str(staged) + os.sep):
        return str(published / Path(value).relative_to(staged))
    return value


def _annotate(entry: dict, source: dict, df: pd.DataFrame, processed_root: Path) -> dict:
    # Inventory corrections describe the same retained artifact; collection and
    # extraction lineage stay pinned to the bytes that actually produced it.
    source_metadata = entry.setdefault("source", {})
    for inventory_key, metadata_key in (("publisher_org", "publisher"), ("dataset_title", "title"),
                                       ("official_flag", "official_flag"), ("license_terms", "license_terms"),
                                       ("publisher_type", "publisher_type"), ("domain", "domain")):
        if inventory_key in source:
            source_metadata[metadata_key] = source[inventory_key]
    if source.get("resource_page_url") or source.get("url"):
        source_metadata["url"] = source.get("resource_page_url") or source["url"]
    scope = LEGACY_SCOPE_METADATA.get(source["source_id"], {}) | {key: source[key] for key in ("agency", "entity_type", "entity_id", "road_class", "scope_note", "source_as_of_date", "publisher_type") if key in source}
    entry["analytical_scope"] = scope or {"road_class": "unspecified", "scope_note": "Use source-specific row definitions; no cross-source identity inferred."}
    evidence = evidence_status(df, source, entry)
    entry["evidence_status"] = evidence
    entry["source_as_of_date"] = observation_date(df, scope | source | entry.get("source", {}) | {"source_as_of_date": entry.get("source_as_of_date") or source.get("source_as_of_date") or scope.get("source_as_of_date")})
    entry["publication_date"] = source.get("publication_date") or entry.get("publication_date") or entry.get("source", {}).get("publication_date")
    if "publication_date" in df and not df["publication_date"].dropna().empty:
        entry["publication_date"] = sorted(df["publication_date"].dropna().astype(str))[-1]
    if "published_at" in df and not df["published_at"].dropna().empty:
        dates = [value for value in df["published_at"].dropna().astype(str) if value]
        if dates:
            entry["publication_date"] = sorted(dates)[-1]
    entry["disclosure_ready"] = (not df.empty and entry.get("metric_category") in {"official_measured", "issuer_disclosed"}
                                  and evidence in {"validated", "verified", "validated_primary", "validated_curated"}
                                  and not semantic_errors(df, source))
    # A verified target or valuation assumption can be read as a disclosure,
    # while measured arithmetic requires explicitly eligible observations.
    entry["analytical_ready"] = entry["disclosure_ready"]
    if "analytical_eligible" in df:
        entry["analytical_ready"] = entry["analytical_ready"] and bool(df["analytical_eligible"].fillna(False).any())
    if source["source_id"] in DOCUMENT_SOURCES:
        entry["analytical_ready"] = False  # presence rows are not extracted accounting facts
        extraction = _load_nhai_extraction_quality(processed_root, entry.get("output_table_path")) if source["source_id"] in NHAI_EXTRACTION_QUALITY_SOURCE_IDS else None
        for key in ("extraction_quality", "extraction_quality_reference", "extraction_quality_score"):
            entry.pop(key, None)
        stale_quality = (processed_root / "nhai_annual_report_tables" / "quality_report.json").exists() and source["source_id"] in NHAI_EXTRACTION_QUALITY_SOURCE_IDS
        retained_documents = extraction and any(row.get("outcome") == "retained_after_failure" for row in extraction.get("document_refresh_outcomes", []))
        entry["extraction_status"] = "extracted_with_retained_documents" if retained_documents else "extracted" if extraction else "checksum_mismatch" if stale_quality else "fetched" if not df.empty else "discovered"
        if extraction:
            entry["extraction_quality"] = extraction
            entry["extraction_quality_reference"] = {key: extraction[key] for key in ("quality_report_path", "extraction_manifest_path", "generated_at", "source_parquet", "source_parquet_sha256", "document_refresh_outcomes")}
    else:
        entry.setdefault("extraction_status", "validated" if entry["analytical_ready"] else "pending")
    context = source | entry.get("source", {}) | {k: entry[k] for k in ("status", "evidence_status", "extraction_status", "source_as_of_date") if k in entry}
    context["source_id"] = source["source_id"]
    if entry.get("extraction_quality"):
        context["extraction_quality"] = entry["extraction_quality"]
    entry.update(evaluate(df, context))
    return entry


def run_ingestion(
    inventory_path: str = "research/source_inventory.yaml", selected_sources: list[str] | None = None,
    raw_root: Path = Path("data/raw"), processed_root: Path = Path("data/processed"),
    manifest_root: Path = Path("data/manifests"), catalog_path: Path = Path("data/manifests/catalog.json"),
) -> Dict[str, dict]:
    inv = load_inventory(inventory_path)
    source_map = {s["source_id"]: s for s in inv.sources}
    if selected_sources and set(selected_sources) - source_map.keys():
        raise ValueError(f"Unknown requested sources: {sorted(set(selected_sources) - source_map.keys())}")
    targets = selected_sources or list(source_map)
    ensure_dirs(raw_root, processed_root, manifest_root, catalog_path.parent)
    entries = {entry["source_id"]: entry for entry in read_json(catalog_path).get("datasets", [])}
    report_path = manifest_root / "refresh_report.json"
    old_report = read_json(report_path)
    outcomes = {row["source_id"]: row for row in old_report.get("sources", [])}
    started = datetime.now(timezone.utc).isoformat()
    for source_id in targets:
        source = source_map[source_id]
        checked = datetime.now(timezone.utc).isoformat()
        output = processed_root / f"{source_id}.parquet"
        previous = copy.deepcopy(entries.get(source_id))
        connector = find_connector_for_source(source_id)
        failure = None
        candidate = None
        candidate_df = pd.DataFrame()
        with tempfile.TemporaryDirectory(prefix="bhai-ingest-") as temp:
            staged_processed, staged_manifests = Path(temp) / "processed", Path(temp) / "manifests"
            ensure_dirs(staged_processed, staged_manifests)
            # Derived connectors can read validated dependencies without writing over them.
            for dependency in processed_root.glob("*.parquet"):
                if dependency.name != output.name:
                    (staged_processed / dependency.name).symlink_to(dependency.resolve())
            try:
                if connector is None:
                    failure = "No connector declared for source"
                else:
                    result = connector.run(source, raw_root, staged_processed, staged_manifests)
                    candidate = copy.deepcopy(result.manifest)
                    candidate.setdefault("output_table_path", str(result.output_table_path))
                    candidate_df = _read_frame(Path(candidate["output_table_path"]))
                    candidate.setdefault("source", {}).setdefault("official_flag", source.get("official_flag", False))
                    issues = semantic_errors(candidate_df, source)
                    if candidate.get("status") not in SUCCESS_STATUSES or candidate_df.empty:
                        failure = candidate.get("skip_reason") or result.skip_reason or "No validated observations retrieved"
                    elif issues:
                        failure = "; ".join(issues)
                    elif evidence_status(candidate_df, source, candidate) == "unverified":
                        failure = "Manual/source evidence has no concrete document, anchor and observation date"
            except Exception as exc:
                failure = f"{type(exc).__name__}: {exc}"
            if failure:
                entry = previous or candidate or _base_manifest(source, output)
                # Preserve the published bytes and timestamps even when network/parser/validation fails.
                entry = _rewrite_paths(entry, staged_processed, processed_root)
                entry["output_table_path"] = str(output)
                if not output.exists():
                    empty = candidate_df.iloc[0:0] if len(candidate_df.columns) else pd.DataFrame(columns=["source_id", "source_type", "metric_category"])
                    write_parquet(empty, output)
                published_df = _read_frame(output)
                if not previous:
                    entry = _base_manifest(source, output) | {k: v for k, v in entry.items() if k not in {"manifest", "output_table_path"}}
                    entry["manifest"] = {"raw_files": [], "row_count": len(published_df), "columns": list(published_df.columns), "output_files": []}
                if not previous and published_df.empty:
                    # A failed request is a check, not a successful retrieval.
                    entry.setdefault("source", {}).pop("retrieved_at", None)
                    entry.pop("retrieved_at", None)
                    entry.pop("last_successful_retrieval_at", None)
                if connector is None:
                    outcome = "not_mapped"
                elif source.get("auth") in {"restricted", "captcha"}:
                    outcome = "restricted"
                elif entry.get("status") == "metadata_only":
                    outcome = "metadata_only"
                elif candidate and candidate.get("status") == "manual_ingest" and evidence_status(candidate_df, source, candidate) == "unverified":
                    outcome = "manual_evidence_required"
                elif previous and not published_df.empty:
                    outcome = "retained_after_failure"
                elif not source.get("allow_auto_fetch"):
                    outcome = "manual_evidence_required"
                else:
                    outcome = "unavailable"
                entry["refresh_error"] = failure
            else:
                old_df = _read_frame(output)
                new_checksum = dataframe_checksum(candidate_df)
                unchanged = output.exists() and dataframe_checksum(old_df) == new_checksum
                entry = _rewrite_paths(candidate, staged_processed, processed_root)
                entry["output_table_path"] = str(output)
                entry["data_checksum"] = new_checksum
                if unchanged:
                    # Retain exact bytes + collection lineage; only the check time advances.
                    entry = copy.deepcopy(previous) if previous else entry
                    entry["data_checksum"] = new_checksum
                    outcome = "checked_unchanged"
                else:
                    for staged_file in staged_processed.rglob("*"):
                        if staged_file.is_file() and not staged_file.is_symlink():
                            target = processed_root / staged_file.relative_to(staged_processed)
                            target.parent.mkdir(parents=True, exist_ok=True)
                            os.replace(staged_file, target)
                    entry["data_changed_at"] = checked
                    outcome = "model_generated" if entry.get("metric_category") == "model_output" else "updated"
                if candidate.get("status") == "manual_ingest":
                    entry["last_successful_retrieval_at"] = candidate.get("last_successful_retrieval_at") or (previous or {}).get("last_successful_retrieval_at") or (previous or candidate).get("source", {}).get("retrieved_at")
                else:
                    entry["last_successful_retrieval_at"] = checked
                entry["last_ingested_at"] = checked
                entry.pop("refresh_error", None)
                published_df = _read_frame(output)
            entry["source_id"] = source_id
            entry.setdefault("metric_category", source.get("metric_category", "official_measured"))
            entry.setdefault("citations", _base_manifest(source, output)["citations"])
            entry.setdefault("manifest", {})["output_files"] = [{"path": str(output), "format": "parquet", "sha256": sha256_for_file(output)}]
            entry["manifest"].update(row_count=len(published_df), columns=list(published_df.columns))
            entry["data_checksum"] = dataframe_checksum(published_df)
            entry["last_checked_at"], entry["refresh_outcome"] = checked, outcome
            entry = _annotate(entry, source, published_df, processed_root)
            if not entry["analytical_ready"] and entry["evidence_status"] == "unverified":
                entry["quarantine_reason"] = failure or "Unverified source artifact; retained for review only"
            else:
                entry.pop("quarantine_reason", None)
            write_json(entry, manifest_root / f"{source_id}.json")
            entries[source_id] = entry
            outcomes[source_id] = {"source_id": source_id, "outcome": outcome, "last_checked_at": checked,
                                   "source_as_of_date": entry.get("source_as_of_date"), "publication_date": entry.get("publication_date"),
                                   "analytical_ready": entry["analytical_ready"], "evidence_status": entry["evidence_status"],
                                   "disclosure_ready": entry["disclosure_ready"],
                                   "extraction_status": entry["extraction_status"],
                                   "row_count": len(published_df), "error": failure}
    write_catalog(catalog_path, list(entries.values()))
    rows = [outcomes[sid] for sid in source_map if sid in outcomes]
    write_json({"generated_at": datetime.now(timezone.utc).isoformat(), "started_at": started,
                "registered_source_count": len(source_map), "checked_source_count": len(rows),
                "run_source_ids": targets, "outcome_counts": dict(Counter(r["outcome"] for r in rows)),
                "analytical_ready_source_count": sum(r["analytical_ready"] for r in rows),
                "disclosure_ready_source_count": sum(r.get("disclosure_ready", False) for r in rows), "sources": rows}, report_path)
    return entries


def refresh_quality_only(inventory_path: str, selected_sources: list[str] | None = None,
                         processed_root: Path = Path("data/processed"),
                         manifest_root: Path = Path("data/manifests"),
                         catalog_path: Path = Path("data/manifests/catalog.json")) -> Dict[str, dict]:
    """Attach extraction results to their exact inputs; never refetch or rewrite parquet."""
    sources = {source["source_id"]: source for source in load_inventory(inventory_path).sources}
    targets = selected_sources or sorted(DOCUMENT_SOURCES & sources.keys())
    entries = {entry["source_id"]: entry for entry in read_json(catalog_path).get("datasets", [])}
    report_path = manifest_root / "refresh_report.json"
    report = read_json(report_path)
    for source_id in targets:
        if source_id not in sources or source_id not in entries:
            raise ValueError(f"No existing source to rescore: {source_id}")
        entry = _annotate(copy.deepcopy(entries[source_id]), sources[source_id], _read_frame(Path(entries[source_id]["output_table_path"])), processed_root)
        entry["last_quality_check_at"] = datetime.now(timezone.utc).isoformat()
        write_json(entry, manifest_root / f"{source_id}.json")
        entries[source_id] = entry
        for row in report.get("sources", []):
            if row["source_id"] == source_id:
                row["analytical_ready"] = entry["analytical_ready"]
                row["disclosure_ready"] = entry["disclosure_ready"]
                row["extraction_status"] = entry["extraction_status"]
                for field in ("source_as_of_date", "publication_date", "evidence_status"):
                    row[field] = entry.get(field)
    write_catalog(catalog_path, list(entries.values()))
    if report:
        report["analytical_ready_source_count"] = sum(row.get("analytical_ready", False) for row in report.get("sources", []))
        report["disclosure_ready_source_count"] = sum(row.get("disclosure_ready", False) for row in report.get("sources", []))
        write_json(report, report_path)
    return entries


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory", default="research/source_inventory.yaml")
    parser.add_argument("--source", action="append", help="run only specified source_ids")
    parser.add_argument("--refresh-quality-only", action="store_true", help="Rescore existing artifacts without network access or parquet mutation")
    args = parser.parse_args()
    if args.refresh_quality_only:
        refresh_quality_only(args.inventory, args.source)
    else:
        run_ingestion(args.inventory, args.source)
    print("Ingestion complete. Catalog and per-source refresh report written to data/manifests.")


if __name__ == "__main__":
    main()
