"""Restore newer validated source outputs without replacing governed manual snapshots."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipelines.common import dataframe_checksum, sha256_for_file, write_json
from pipelines.quality import semantic_errors
from research.loader import load_inventory


def _json(path: Path) -> dict:
    try:
        payload = json.loads(path.read_text())
        return payload if isinstance(payload, dict) else {}
    except (OSError, ValueError):
        return {}


def _changed(entry: dict) -> datetime:
    try:
        value = datetime.fromisoformat(str(entry.get("data_changed_at") or "").replace("Z", "+00:00"))
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
    except (KeyError, ValueError, TypeError):
        return datetime.min.replace(tzinfo=timezone.utc)


def _copy(source: Path, target: Path) -> None:
    if source.is_symlink():
        raise ValueError(f"Symlink is not a validated source file: {source}")
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temp = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent)
    os.close(fd)
    try:
        shutil.copyfile(source, temp)
        os.replace(temp, target)
    finally:
        Path(temp).unlink(missing_ok=True)


def _manual_hash(workspace: Path, source_id: str) -> str | None:
    csv = workspace / "data/raw/manual" / f"{source_id}.csv"
    evidence = _json(workspace / "data/raw/manual/evidence" / f"{source_id}.json")
    if csv.is_file() and evidence.get("csv_sha256") == sha256_for_file(csv):
        return evidence["csv_sha256"]
    return None


def _entry_csv_hash(entry: dict, source_id: str) -> str | None:
    suffix = f"raw/manual/{source_id}.csv"
    return next((item.get("sha256") for item in entry.get("manifest", {}).get("raw_files", [])
                 if str(item.get("path", "")).replace("\\", "/").endswith(suffix)), None)


def _validated(data: Path, workspace: Path, source: dict) -> tuple[dict, str | None]:
    source_id = source["source_id"]
    entry = _json(data / "manifests" / f"{source_id}.json")
    parquet = data / "processed" / f"{source_id}.parquet"
    try:
        if entry.get("source_id") != source_id or not parquet.is_file() or parquet.is_symlink():
            raise ValueError("Missing source manifest or output")
        output_hashes = [item.get("sha256") for item in entry.get("manifest", {}).get("output_files", [])
                         if str(item.get("path", "")).replace("\\", "/").endswith(f"processed/{source_id}.parquet")]
        if output_hashes != [sha256_for_file(parquet)]:
            raise ValueError("Published output checksum mismatch")
        frame = pd.read_parquet(parquet)
        metadata = entry.get("manifest", {})
        if len(frame) != metadata.get("row_count") or list(frame.columns) != metadata.get("columns"):
            raise ValueError("Published output schema/row count mismatch")
        if entry.get("data_checksum") != dataframe_checksum(frame):
            raise ValueError("Published content checksum mismatch")
        governed = _manual_hash(workspace, source_id)
        if governed and not frame.empty and _entry_csv_hash(entry, source_id) != governed:
            raise ValueError("Output belongs to a different governed manual snapshot")
        if entry.get("analytical_ready") or entry.get("disclosure_ready"):
            if entry.get("evidence_status") not in {"verified", "validated", "validated_primary", "validated_curated"}:
                raise ValueError("Readiness lacks validated evidence")
            issues = semantic_errors(frame, source)
            if issues:
                raise ValueError("; ".join(issues))
        if not frame.empty and {"entity_id", "citation_url", "source_document_sha256", "analytical_eligible"} <= set(frame.columns):
            from pipelines.connectors.primary_disclosures import validate_facts
            evidence = _json(workspace / "data/raw/manual/evidence" / f"{source_id}.json")
            validate_facts(frame.copy(), source_id, evidence)
        return entry, None
    except (OSError, ValueError, KeyError, TypeError) as exc:
        return entry, str(exc)


def _ocr_bundle(data: Path, selected_source: Path) -> tuple[dict, bool]:
    folder = data / "processed/nhai_annual_report_tables"
    manifest = _json(folder / "extraction_manifest.json")
    quality = _json(folder / "quality_report.json")
    canonical = data / "processed/nhai_annual_report_tables_canonical.parquet"
    try:
        source_hash = sha256_for_file(selected_source)
        valid = (manifest.get("source_parquet_sha256") == source_hash
                 and quality.get("source_parquet_sha256") == source_hash
                 and manifest.get("canonical", {}).get("sha256") == sha256_for_file(canonical)
                 and manifest.get("rows_merged") == quality.get("canonical_rows") == len(pd.read_parquet(canonical)))
        return manifest, valid
    except (OSError, ValueError):
        return manifest, False


def restore_workspace(prior_root: Path, workspace: Path, inventory: Path) -> dict:
    """Select validated per-source generations, using this revision's manual evidence."""
    current_data = workspace / "data"
    prior_data = prior_root / "data"
    if not prior_data.is_dir():
        raise ValueError("Prior research artifact must contain data/")
    sources = load_inventory(inventory).sources
    catalog = _json(current_data / "manifests/catalog.json")
    entries = {entry["source_id"]: entry for entry in catalog.get("datasets", [])}
    decisions = []
    for source in sources:
        sid = source["source_id"]
        if not re.fullmatch(r"[A-Za-z0-9_-]+", sid):
            raise ValueError("Unsafe source ID")
        current, current_error = _validated(current_data, workspace, source)
        prior, prior_error = _validated(prior_data, workspace, source)
        use_prior = prior_error is None and (current_error is not None or _changed(prior) > _changed(current))
        if use_prior:
            _copy(prior_data / "processed" / f"{sid}.parquet", current_data / "processed" / f"{sid}.parquet")
            write_json(prior, current_data / "manifests" / f"{sid}.json")
            entries[sid] = prior
        elif current:
            # Ingestion reads its previous source generation from the catalog.
            entries[sid] = current
        decisions.append({"source_id": sid, "selected": "prior" if use_prior else "current",
                          "current_data_changed_at": current.get("data_changed_at"),
                          "prior_data_changed_at": prior.get("data_changed_at"),
                          "current_validation_error": current_error, "prior_validation_error": prior_error})
    write_json({"generated_at": datetime.now(timezone.utc).isoformat(),
                "datasets": list(entries.values())}, current_data / "manifests/catalog.json")
    selected_source = current_data / "processed/nhai_annual_report_documents.parquet"
    current_ocr, current_valid = _ocr_bundle(current_data, selected_source)
    prior_ocr, prior_valid = _ocr_bundle(prior_data, selected_source)
    def generated(payload):
        return _changed({"data_changed_at": payload.get("generated_at")})
    restore_ocr = prior_valid and (not current_valid or generated(prior_ocr) > generated(current_ocr))
    if restore_ocr:
        folder = Path("processed/nhai_annual_report_tables")
        for path in sorted((prior_data / folder).rglob("*")):
            if path.is_file():
                _copy(path, current_data / path.relative_to(prior_data))
        _copy(prior_data / "processed/nhai_annual_report_tables_canonical.parquet",
              current_data / "processed/nhai_annual_report_tables_canonical.parquet")
    report = {"generated_at": datetime.now(timezone.utc).isoformat(), "sources": decisions,
              "restored_source_count": sum(row["selected"] == "prior" for row in decisions),
              "restored_matching_ocr_bundle": restore_ocr, "raw_manual_files_preserved": True}
    write_json(report, current_data / "manifests/workspace_restore_report.json")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prior", type=Path, required=True)
    parser.add_argument("--workspace", type=Path, default=Path("."))
    parser.add_argument("--inventory", type=Path, default=Path("research/source_inventory.yaml"))
    args = parser.parse_args()
    report = restore_workspace(args.prior, args.workspace, args.inventory)
    print(json.dumps({key: value for key, value in report.items() if key != "sources"}, indent=2))


if __name__ == "__main__":
    main()
