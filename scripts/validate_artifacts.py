#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List
import sys

import hashlib
import pandas as pd
import re

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipelines.morth_appendix_validation import compare_appendix2_to_reference, validate_appendix2_snapshot
from pipelines.quality import semantic_errors
from pipelines.ingest import REFRESH_OUTCOMES


def _read_json(path: Path) -> Dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _load_yaml(path: Path):
    import yaml

    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _validate_raw_lineage(entry: Dict, errors: List[str], raw_root: Path | None = None) -> None:
    raw_root=(raw_root or ROOT/"data/raw").resolve()
    def resolved_input(value: str) -> Path:
        path=Path(value)
        if path.is_absolute():
            return path.resolve()
        # Manifest input paths are repository-relative, while source document
        # paths are relative to data/raw. Resolve both spellings to one identity.
        if path.parts[:2]==("data","raw"):
            return raw_root.joinpath(*path.parts[2:]).resolve()
        return (ROOT/path).resolve()
    metadata=entry.get("manifest", {})
    declared_gaps={(resolved_input(item["path"]),item.get("sha256")) for item in metadata.get("raw_evidence_gaps", [])
                   if item.get("path")
                   if item.get("reason") in {"primary_document_archive_missing","raw_input_archive_missing"} and item.get("availability")=="not_archived"}
    for item in metadata.get("raw_files", []):
        path=resolved_input(item.get("path", ""))
        if path.is_file():
            if _sha256(path)!=item.get("sha256"):
                errors.append(f"Source {entry.get('source_id')} raw input checksum mismatch: {path}")
            if item.get("size_bytes") is not None and path.stat().st_size!=item["size_bytes"]:
                errors.append(f"Source {entry.get('source_id')} raw input byte length mismatch: {path}")
        elif (path,item.get("sha256")) not in declared_gaps:
            errors.append(f"Source {entry.get('source_id')} raw input missing without an explicit document gap: {path}")
    for item in metadata.get("unbound_raw_evidence", []):
        if entry.get("analytical_ready") or entry.get("evidence_status") not in {"unverified","manual_unverified","synthetic"} and entry.get("metric_category") not in {"proxy_derived","model_output"}:
            errors.append(f"Source {entry.get('source_id')} validated data cannot waive a mismatched input binding")
        path=resolved_input(item.get("path", ""))
        if item.get("binding_status")!="quarantined" or (path.is_file() and _sha256(path)!=item.get("available_sha256")):
            errors.append(f"Source {entry.get('source_id')} unbound evidence availability/checksum mismatch: {path}")
    for document in metadata.get("source_documents", []):
        if not document.get("relative_path"):
            continue
        path=(raw_root/document["relative_path"]).resolve()
        if path.is_file() and _sha256(path)!=document.get("sha256"):
            errors.append(f"Source {entry.get('source_id')} primary document checksum mismatch: {path}")
        elif not path.is_file() and (document.get("availability")!="not_archived" or (path,document.get("sha256")) not in declared_gaps):
            errors.append(f"Source {entry.get('source_id')} primary document missing without an explicit archive gap: {path}")


def _validate_entry(entry: Dict, manifest_root: Path, errors: List[str], warnings: List[str]) -> None:
    source_id = entry.get("source_id")
    if not source_id:
        errors.append("Catalog entry missing source_id")
        return
    _validate_raw_lineage(entry,errors)

    manifest = manifest_root / f"{source_id}.json"
    if not manifest.exists():
        errors.append(f"Missing per-source manifest: {source_id}")

    required_fields = ["source_id", "status", "metric_category", "source", "citations", "manifest", "overall_confidence_badge", "output_table_path", "analytical_ready", "disclosure_ready", "evidence_status", "extraction_status", "last_checked_at", "refresh_outcome"]
    for field in required_fields:
        if field not in entry:
            errors.append(f"Source {source_id} missing required catalog field: {field}")

    source_meta = entry.get("source", {})
    for field in ["publisher", "license_terms"] + (["retrieved_at"] if entry.get("analytical_ready") else []):
        if not source_meta.get(field):
            warnings.append(f"Source {source_id} missing source.{field}")

    citations = entry.get("citations", {})
    for field in ["permanent_identifier", "anchor"]:
        if not citations.get(field):
            errors.append(f"Source {source_id} missing citations.{field}")

    if entry.get("metric_category") == "model_output" and source_meta.get("official_flag") is not False:
        warnings.append(f"Model output source {source_id} should keep source.official_flag=false")

    if entry.get("metric_category", "").startswith("proxy") and source_meta.get("official_flag") is not False:
        warnings.append(f"Proxy source {source_id} should keep source.official_flag=false")

    if entry.get("metric_category") not in {"official_measured", "issuer_disclosed", "proxy_derived", "model_output"}:
        errors.append(f"Source {source_id} has non-standard metric_category: {entry.get('metric_category')}")
    if entry.get("refresh_outcome") not in REFRESH_OUTCOMES:
        errors.append(f"Source {source_id} has unknown refresh outcome: {entry.get('refresh_outcome')}")
    if entry.get("analytical_ready") and entry.get("evidence_status") not in {"verified", "validated", "validated_primary", "validated_curated"}:
        errors.append(f"Source {source_id} analytical readiness has no verified evidence")
    if entry.get("disclosure_ready") and entry.get("evidence_status") not in {"verified", "validated", "validated_primary", "validated_curated"}:
        errors.append(f"Source {source_id} disclosure readiness has no verified evidence")
    if entry.get("metric_category") in {"proxy_derived", "model_output"} and entry.get("analytical_ready"):
        errors.append(f"Source {source_id} proxy/synthetic values cannot enter measured analyst calculations")

    output_path = Path(entry.get("output_table_path")) if entry.get("output_table_path") else None
    if output_path and output_path.exists():
        output_size = output_path.stat().st_size
        if output_size <= 0:
            warnings.append(f"Source {source_id} output parquet is empty ({output_path})")
        try:
            frame = pd.read_parquet(output_path)
            if len(frame) != entry.get("manifest", {}).get("row_count"):
                errors.append(f"Source {source_id} manifest row count differs from parquet")
            if list(frame.columns) != entry.get("manifest", {}).get("columns"):
                errors.append(f"Source {source_id} manifest schema differs from parquet")
            if entry.get("analytical_ready"):
                errors.extend(f"Source {source_id}: {issue}" for issue in semantic_errors(frame, {"source_id": source_id}))
            if {"entity_id", "citation_url", "source_document_sha256", "analytical_eligible"} <= set(frame.columns) and not frame.empty:
                from pipelines.connectors.primary_disclosures import validate_facts
                evidence_path = ROOT / "data/raw/manual/evidence" / f"{source_id}.json"
                if not evidence_path.exists():
                    errors.append(f"Source {source_id} has no evidence manifest for financial facts")
                else:
                    validate_facts(frame, source_id, _read_json(evidence_path),entry.get("research_cutoff"))
            if source_id == "correlation_matrix" and not frame.empty:
                if (pd.to_numeric(frame["overlap_records"], errors="coerce") < 10).any():
                    errors.append("Published correlation has fewer than ten compatible observations")
                if not entry.get("derivation_inputs"):
                    errors.append("Published correlation is missing input lineage")
        except Exception as exc:
            errors.append(f"Source {source_id} integrity validation failed: {exc}")
        if source_id == "data_gov_in_nhai_stateut_project_delay_status_2024":
            try:
                df = pd.read_parquet(output_path)
                aliases = {
                    "state": ["state/ut", "state_ut", "state", "states/ut"],
                    "projects": ["number_of_projects", "projects", "total_projects"],
                    "delayed": ["number_of_delayed_projects", "delayed_projects", "projects_delayed"],
                }
                resolved = _resolve_column_aliases(list(df.columns), aliases)
                resolved.setdefault("state", _resolve_column_by_tokens(list(df.columns), ["state"]))
                resolved.setdefault("projects", _resolve_column_by_tokens(list(df.columns), ["project"]))
                resolved.setdefault("delayed", _resolve_column_by_tokens(list(df.columns), ["delay", "project"]))
                missing_cols = [logical_name for logical_name in aliases if not resolved.get(logical_name)]
                if missing_cols:
                    errors.append(f"Source {source_id} missing required logical columns: {sorted(missing_cols)}")
                else:
                    state_col = resolved["state"]
                    project_col = resolved["projects"]
                    delayed_col = resolved["delayed"]
                    states = df[state_col].astype(str).str.strip()
                    core = df.loc[~states.str.lower().isin({"total", "india", "all india"})].copy()
                    if len(core) < 30:
                        errors.append(f"Source {source_id} has insufficient state/UT coverage: {len(core)} rows")
                    projects = pd.to_numeric(core[project_col], errors="coerce")
                    delayed = pd.to_numeric(core[delayed_col], errors="coerce")
                    if projects.isna().any() or delayed.isna().any():
                        errors.append(f"Source {source_id} contains non-numeric project counts")
                    if (projects < 0).any() or (delayed < 0).any():
                        errors.append(f"Source {source_id} contains negative project counts")
                    if (delayed > projects).any():
                        errors.append(f"Source {source_id} has delayed project counts above total projects")
                    if not states.str.lower().eq("total").any():
                        warnings.append(f"Source {source_id} is missing a Total row")
            except Exception as exc:
                errors.append(f"Source {source_id} state/UT delay validation could not be executed: {exc}")
        if source_id == "morth_annual_report_pdf":
            try:
                df = pd.read_parquet(output_path)
                appendix_result = validate_appendix2_snapshot(df)
                for item in appendix_result.errors:
                    errors.append(f"Source {source_id} appendix2 validation: {item}")
                for item in appendix_result.warnings:
                    warnings.append(f"Source {source_id} appendix2 validation: {item}")
                reference_path = Path("data/raw/manual/morth_annual_report_pdf_validation_2025.csv")
                reference_result = compare_appendix2_to_reference(df, reference_path)
                for item in reference_result.get("errors", []):
                    errors.append(f"Source {source_id} parliamentary cross-check: {item}")
                for item in reference_result.get("warnings", []):
                    warnings.append(f"Source {source_id} parliamentary cross-check: {item}")
            except Exception as exc:
                errors.append(f"Source {source_id} appendix2 validation could not be executed: {exc}")
        if source_id == "data_gov_in_nh_fatalities_injuries_state_year":
            try:
                df = pd.read_parquet(output_path)
                aliases = {
                    "state": ["states/ut", "state/ut", "state", "states_ut"],
                    "fatalities_2020": ["fatalities_-_2020", "fatalities_2020", "fatalities2020"],
                    "fatalities_2021": ["fatalities_-_2021", "fatalities_2021", "fatalities2021"],
                    "fatalities_2022": ["fatalities_-_2022", "fatalities_2022", "fatalities2022"],
                    "injuries_2020": ["injuries_-_2020", "injuries_2020", "injuries2020"],
                    "injuries_2021": ["injuries_-_2021", "injuries_2021", "injuries2021"],
                    "injuries_2022": ["injuries_-_2022", "injuries_2022", "injuries2022"],
                }
                resolved = _resolve_column_aliases(list(df.columns), aliases)
                resolved.setdefault("state", _resolve_column_by_tokens(list(df.columns), ["state"]))
                resolved.setdefault("fatalities_2020", _resolve_column_by_tokens(list(df.columns), ["fatal", "2020"]))
                resolved.setdefault("fatalities_2021", _resolve_column_by_tokens(list(df.columns), ["fatal", "2021"]))
                resolved.setdefault("fatalities_2022", _resolve_column_by_tokens(list(df.columns), ["fatal", "2022"]))
                resolved.setdefault("injuries_2020", _resolve_column_by_tokens(list(df.columns), ["injur", "2020"]))
                resolved.setdefault("injuries_2021", _resolve_column_by_tokens(list(df.columns), ["injur", "2021"]))
                resolved.setdefault("injuries_2022", _resolve_column_by_tokens(list(df.columns), ["injur", "2022"]))
                missing_cols = [logical_name for logical_name in aliases if logical_name not in resolved]
                if missing_cols:
                    errors.append(f"Source {source_id} missing required logical columns: {sorted(missing_cols)}")
                else:
                    state_col = resolved["state"]
                    core = df.loc[~df[state_col].astype(str).str.strip().str.lower().isin({"total", "india", "all india"})].copy()
                    if core[state_col].nunique() < 35:
                        errors.append(f"Source {source_id} has insufficient state/UT coverage: {core[state_col].nunique()} unique states")
                    for logical_name in ["fatalities_2020", "fatalities_2021", "fatalities_2022", "injuries_2020", "injuries_2021", "injuries_2022"]:
                        col = resolved[logical_name]
                        vals = pd.to_numeric(core[col], errors="coerce")
                        if vals.isna().all():
                            errors.append(f"Source {source_id} contains no usable numeric values in {col}")
                        elif vals.isna().any():
                            missing_states = set(core.loc[vals.isna(), state_col].astype(str).str.strip())
                            allowed_missing_states = {"Ladakh"} if logical_name.endswith("2020") else set()
                            if missing_states - allowed_missing_states:
                                warnings.append(f"Source {source_id} contains partial missing values in {col}")
                        if (vals < 0).any():
                            errors.append(f"Source {source_id} contains negative values in {col}")
                    if core.duplicated(subset=[state_col]).any():
                        errors.append(f"Source {source_id} contains duplicate state rows")
            except Exception as exc:
                errors.append(f"Source {source_id} NH fatalities/injuries validation could not be executed: {exc}")
        if source_id == "parliament_qa_nh_blackspots_state":
            try:
                df = pd.read_parquet(output_path)
                required_cols = {"state", "nh_blackspots", "nh_blackspot_accidents", "nh_blackspot_fatalities", "rectified_blackspots", "source_as_of_date"}
                missing_cols = required_cols - set(df.columns)
                if missing_cols:
                    errors.append(f"Source {source_id} missing required columns: {sorted(missing_cols)}")
                else:
                    core = df.loc[~df["state"].astype(str).str.strip().str.lower().isin({"total", "india", "all india"})].copy()
                    if len(core) < 30:
                        warnings.append(f"Source {source_id} limited state/UT coverage: {len(core)} rows")
                    for col in ["nh_blackspots", "nh_blackspot_accidents", "nh_blackspot_fatalities", "rectified_blackspots"]:
                        vals = pd.to_numeric(core[col], errors="coerce")
                        if vals.isna().any():
                            errors.append(f"Source {source_id} contains non-numeric values in {col}")
                        if (vals < 0).any():
                            errors.append(f"Source {source_id} contains negative values in {col}")
                    blackspots = pd.to_numeric(core["nh_blackspots"], errors="coerce")
                    rectified = pd.to_numeric(core["rectified_blackspots"], errors="coerce")
                    if (rectified > blackspots).any():
                        errors.append(f"Source {source_id} has rectified black spots above total black spots")
                    if core.duplicated(subset=["state"]).any():
                        errors.append(f"Source {source_id} contains duplicate state rows")
            except Exception as exc:
                errors.append(f"Source {source_id} NH blackspots validation could not be executed: {exc}")
        if source_id == "nhai_constructed_length_series_official":
            try:
                df = pd.read_parquet(output_path)
                required_cols = {"period", "km_constructed", "series_scope", "series_status", "source_url", "citation_anchor", "document_section", "source_as_of_date"}
                missing_cols = required_cols - set(df.columns)
                if missing_cols:
                    errors.append(f"Source {source_id} missing required columns: {sorted(missing_cols)}")
                else:
                    if df.duplicated(subset=["period"]).any():
                        errors.append(f"Source {source_id} contains duplicate period rows")
                    periods = df["period"].astype(str).str.strip()
                    if periods.nunique() < 10:
                        errors.append(f"Source {source_id} has insufficient year coverage: {periods.nunique()} periods")
                    vals = pd.to_numeric(df["km_constructed"], errors="coerce")
                    if vals.isna().any():
                        errors.append(f"Source {source_id} contains non-numeric km_constructed values")
                    if (vals < 0).any():
                        errors.append(f"Source {source_id} contains negative km_constructed values")
                    statuses = set(df["series_status"].astype(str).str.strip().str.lower())
                    if not statuses <= {"final", "provisional"}:
                        errors.append(f"Source {source_id} contains unsupported series_status values: {sorted(statuses)}")
                    scopes = set(df["series_scope"].astype(str).str.strip())
                    if scopes != {"NHAI-only"}:
                        errors.append(f"Source {source_id} must remain NHAI-only scoped, found: {sorted(scopes)}")
                    if not df["source_url"].astype(str).str.startswith("http").all():
                        errors.append(f"Source {source_id} contains non-URL source_url values")
                    provisional = df.loc[df["series_status"].astype(str).str.lower() == "provisional"]
                    if len(provisional) > 1:
                        warnings.append(f"Source {source_id} contains multiple provisional points")
            except Exception as exc:
                errors.append(f"Source {source_id} construction-series validation could not be executed: {exc}")
        if source_id == "data_gov_in_gsdp_stateut_current_prices_2017_23":
            try:
                df = pd.read_parquet(output_path)
                year_cols = [
                    "gross_state_domestic_product_gsdpat_current_prices_-_2017-18",
                    "gross_state_domestic_product_gsdpat_current_prices_-_2018-19",
                    "gross_state_domestic_product_gsdpat_current_prices_-_2019-20",
                    "gross_state_domestic_product_gsdpat_current_prices_-_2020-21",
                    "gross_state_domestic_product_gsdpat_current_prices_-_2021-22",
                    "gross_state_domestic_product_gsdpat_current_prices_-_2022-23",
                ]
                required_cols = {"state/ut", *year_cols}
                missing_cols = required_cols - set(df.columns)
                if missing_cols:
                    errors.append(f"Source {source_id} missing required columns: {sorted(missing_cols)}")
                else:
                    states = df["state/ut"].astype(str).str.strip()
                    core = df.loc[~states.str.lower().isin({"total", "india", "all india"})].copy()
                    if core["state/ut"].nunique() < 30:
                        errors.append(f"Source {source_id} has insufficient state/UT coverage: {core['state/ut'].nunique()} unique states")
                    if core.duplicated(subset=["state/ut"]).any():
                        errors.append(f"Source {source_id} contains duplicate state rows")
                    latest_values_present = 0
                    for _, row in core.iterrows():
                        row_has_value = False
                        for col in year_cols:
                            value = pd.to_numeric(str(row[col]).replace(",", ""), errors="coerce")
                            if pd.notna(value):
                                if value < 0:
                                    errors.append(f"Source {source_id} contains negative GSDP values in {col}")
                                row_has_value = True
                        if row_has_value:
                            latest_values_present += 1
                    if latest_values_present < len(core):
                        errors.append(f"Source {source_id} has rows without any usable current-price GSDP value")
            except Exception as exc:
                errors.append(f"Source {source_id} GSDP validation could not be executed: {exc}")
    else:
        errors.append(f"Source {source_id} missing output parquet: {entry.get('output_table_path')}")

    if manifest.exists():
        manifest_payload = _read_json(manifest)
        output_files = manifest_payload.get("manifest", {}).get("output_files", [])
        if not output_files:
            errors.append(f"Source {source_id} manifest has no output_files")
        else:
            for item in output_files:
                path = Path(str(item.get("path", "")))
                sha = item.get("sha256")
                if not path.exists():
                    errors.append(f"Source {source_id} manifest output file missing: {path}")
                    continue
                if sha and sha != _sha256(path):
                    errors.append(f"Source {source_id} manifest sha mismatch: {path}")
    for generation in entry.get("publication_generations", []):
        path = Path(str(generation.get("path", "")))
        if not path.is_file() or _sha256(path) != generation.get("sha256"):
            errors.append(f"Source {source_id} archived generation checksum mismatch: {path}")
        else:
            from pipelines.common import dataframe_checksum
            if dataframe_checksum(pd.read_parquet(path)) != generation.get("observation_checksum"):
                errors.append(f"Source {source_id} archived observation checksum mismatch: {path}")


def _validate_metric_registry(payload: dict, inventory_ids: set, catalog: list, errors: list) -> None:
    if not payload:
        if any("metric_coverage" in entry for entry in catalog):
            errors.append("Metric coverage registry is missing")
        return  # Compatibility with artifacts predating metric coverage.
    ids = [source.get("source_id") for source in payload.get("sources", [])]
    if set(ids) != inventory_ids or len(ids) != len(set(ids)):
        errors.append("Metric coverage must account for every source exactly once")
    if payload.get("registered_source_count") != len(inventory_ids):
        errors.append("Metric coverage registered source count differs from inventory")
    metrics = payload.get("metrics", [])
    if {row.get("source_id") for row in metrics} != inventory_ids:
        errors.append("Metric coverage has missing or unknown source IDs")
    cutoff = payload.get("research_cutoff")
    for row in metrics:
        if not cutoff or (row.get("latest_observation_date") and row["latest_observation_date"] > cutoff):
            errors.append(f"Metric coverage exceeds run cutoff: {row.get('source_id')}/{row.get('metric')}")
        if row.get("coverage_status") == "publication_pending" and not row.get("next_expected_publication_at"):
            errors.append("Publication pending requires a source-supported next publication date")
    by_source = {sid: [row for row in metrics if row.get("source_id") == sid] for sid in inventory_ids}
    for entry in catalog:
        if entry.get("source_id") in inventory_ids and entry.get("metric_coverage") != by_source[entry["source_id"]]:
            errors.append(f"Catalog metric coverage differs from registry: {entry['source_id']}")


def _normalize_state_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).strip().lower())


def _resolve_column_aliases(columns: List[str], aliases: Dict[str, List[str]]) -> Dict[str, str]:
    normalized = {_normalize_state_key(col): col for col in columns}
    resolved: Dict[str, str] = {}
    for logical_name, candidates in aliases.items():
        for candidate in candidates:
            match = normalized.get(_normalize_state_key(candidate))
            if match:
                resolved[logical_name] = match
                break
    return resolved


def _resolve_column_by_tokens(columns: List[str], required_tokens: List[str]) -> str | None:
    normalized_pairs = [(_normalize_state_key(col), col) for col in columns]
    for normalized, original in normalized_pairs:
        if all(token in normalized for token in required_tokens):
            return original
    return None


def _dashboard_contract_errors(app_text: str) -> List[str]:
    errors: List[str] = []
    forbidden_titles = [
        "GDP & Infrastructure Context",
        "Model Risk Trajectory by State (proxy-informed)",
    ]
    for title in forbidden_titles:
        if title in app_text:
            errors.append(f"Dashboard contains forbidden legacy title: {title}")

    required_titles = [
        "NH Fatality Trend by State/UT (official)",
        "Economic Scale vs NH Extent by State/UT",
        "Delay Burden Relative to Economic Scale",
    ]
    for title in required_titles:
        if title not in app_text:
            errors.append(f"Dashboard is missing required title: {title}")

    required_markers = [
        "Official NH fatalities: ${",
        "analytics?.accidentLatestYear",
        "Common GSDP period: ${",
        "analytics?.gsdpRows?.[0]?.gsdp_year",
        "Common-period GSDP at current prices (₹ crore)",
        "NH length (km)",
        "Delayed NH projects",
        "missing states are excluded",
    ]
    for marker in required_markers:
        if marker not in app_text:
            errors.append(f"Dashboard is missing required semantic marker: {marker}")
    return errors


def _validate_dashboard_semantics(errors: List[str], warnings: List[str]) -> None:
    app_path = ROOT / "apps/web/src/app.js"
    if not app_path.exists():
        errors.append("Dashboard app.js is missing")
        return
    app_text = app_path.read_text(encoding="utf-8")
    errors.extend(_dashboard_contract_errors(app_text))

    model_path = ROOT / "data/processed/highway_project_risk_and_access_panel.parquet"
    if model_path.exists():
        try:
            model_df = pd.read_parquet(model_path, columns=["state_assigned", "safety_risk_score"])
            states = model_df["state_assigned"].dropna().astype(str).map(_normalize_state_key)
            score_vals = pd.to_numeric(model_df["safety_risk_score"], errors="coerce").dropna()
            if states.nunique() < 10 or score_vals.nunique() <= 1:
                if "Synthetic Risk Scenario Panel (hidden pending better coverage)" not in app_text:
                    errors.append("Synthetic model panel must be hidden when model coverage is sparse or scores are degenerate")
        except Exception as exc:
            warnings.append(f"Could not evaluate synthetic model panel readiness against app semantics: {exc}")

    gsdp_path = ROOT / "data/processed/data_gov_in_gsdp_stateut_current_prices_2017_23.parquet"
    delay_path = ROOT / "data/processed/data_gov_in_nhai_stateut_project_delay_status_2024.parquet"
    morth_path = ROOT / "data/processed/morth_annual_report_pdf.parquet"
    if gsdp_path.exists() and delay_path.exists() and morth_path.exists():
        try:
            gsdp_df = pd.read_parquet(gsdp_path)
            delay_df = pd.read_parquet(delay_path)
            morth_df = pd.read_parquet(morth_path, columns=["state", "metric_name"])
            gsdp_state_col = _resolve_column_aliases(list(gsdp_df.columns), {"state": ["state/ut", "state_ut", "state", "states/ut"]}).get("state")
            delay_state_col = _resolve_column_aliases(list(delay_df.columns), {"state": ["state/ut", "state_ut", "state", "states/ut"]}).get("state")
            if not gsdp_state_col or not delay_state_col:
                warnings.append("Cross-source economic overlap validation could not resolve state columns")
                return
            gsdp_states = {
                _normalize_state_key(value)
                for value in gsdp_df[gsdp_state_col].dropna().astype(str)
                if _normalize_state_key(value) not in {"total", "india", "allindia"}
            }
            delay_states = {
                _normalize_state_key(value)
                for value in delay_df[delay_state_col].dropna().astype(str)
                if _normalize_state_key(value) not in {"total", "india", "allindia"}
            }
            morth_states = {
                _normalize_state_key(value)
                for value in morth_df.loc[morth_df["metric_name"] == "appendix2_statewise_nh_length_km", "state"].dropna().astype(str)
                if _normalize_state_key(value) not in {"total", "india", "allindia"}
            }
            gsdp_delay_overlap = len(gsdp_states & delay_states) / max(1, min(len(gsdp_states), len(delay_states)))
            gsdp_morth_overlap = len(gsdp_states & morth_states) / max(1, min(len(gsdp_states), len(morth_states)))
            if gsdp_delay_overlap < 0.85:
                errors.append(f"GSDP vs delayed-project state overlap too low: {gsdp_delay_overlap:.2f}")
            if gsdp_morth_overlap < 0.85:
                errors.append(f"GSDP vs NH-length state overlap too low: {gsdp_morth_overlap:.2f}")
        except Exception as exc:
            warnings.append(f"Cross-source economic overlap validation could not be executed: {exc}")


def _validate_deploy_docs_and_workflow(errors: List[str], warnings: List[str]) -> None:
    workflow_path = ROOT / ".github/workflows/github-pages.yml"
    readme_path = ROOT / "README.md"
    if not workflow_path.exists():
        errors.append("GitHub Pages workflow is missing")
        return
    workflow_text = workflow_path.read_text(encoding="utf-8")
    required_workflow_markers = [
        "REQUIRED_PAGES_BUILD_TYPE: 'legacy'",
        "REQUIRED_PAGES_BRANCH: 'gh-pages'",
        "REQUIRED_PAGES_PATH: '/'",
        "Missing PAGES_DEPLOY_TOKEN secret.",
        "Publish built site to gh-pages branch",
        "gh api repos/${GITHUB_REPOSITORY}/pages --jq .build_type",
    ]
    for marker in required_workflow_markers:
        if marker not in workflow_text:
            errors.append(f"Pages workflow is missing required marker: {marker}")

    if not readme_path.exists():
        warnings.append("README.md is missing; deploy assumptions are undocumented")
        return
    readme_text = readme_path.read_text(encoding="utf-8")
    forbidden_readme_markers = [
        "set source to **GitHub Actions**",
    ]
    for marker in forbidden_readme_markers:
        if marker in readme_text:
            errors.append(f"README contains outdated Pages guidance: {marker}")

    required_readme_markers = [
        "build_type`: `legacy`",
        "source branch: `gh-pages`",
        "source path: `/`",
        "`PAGES_DEPLOY_TOKEN`",
        "deploy from a branch",
    ]
    for marker in required_readme_markers:
        if marker not in readme_text:
            errors.append(f"README is missing required Pages documentation marker: {marker}")


def run(inventory_path: str, catalog_path: str, manifests_dir: str, fail_on_warning: bool = False) -> int:
    errors: List[str] = []
    warnings: List[str] = []

    inventory_data = _load_yaml(Path(inventory_path))
    if not inventory_data:
        errors.append("source inventory yaml could not be loaded")
        return print_result(errors, warnings, fail_on_warning)

    inventory_ids = {item.get("source_id") for item in inventory_data.get("sources", []) if item.get("source_id")}

    catalog = _read_json(Path(catalog_path)).get("datasets", [])
    if not catalog:
        errors.append("Catalog is empty or missing")
        return print_result(errors, warnings, fail_on_warning)

    catalog_ids = set()
    for entry in catalog:
        sid = entry.get("source_id")
        if not sid:
            continue
        if sid in catalog_ids:
            errors.append(f"Duplicate catalog source: {sid}")
        catalog_ids.add(sid)
        _validate_entry(entry, Path(manifests_dir), errors, warnings)

    missing = sorted(inventory_ids - catalog_ids)
    for sid in missing:
        errors.append(f"Inventory source missing from catalog: {sid}")

    for sid in sorted(catalog_ids - inventory_ids):
        if sid != "correlation_matrix":
            errors.append(f"Catalog has non-inventory source: {sid}")

    machine_path = Path(inventory_path).with_suffix(".json")
    machine_ids = {source.get("source_id") for source in _read_json(machine_path).get("sources", [])}
    if machine_ids != inventory_ids:
        errors.append("Machine source inventory does not match all human inventory source IDs")
    refresh = _read_json(Path(manifests_dir) / "refresh_report.json")
    refreshed = {source.get("source_id") for source in refresh.get("sources", [])}
    if refreshed != inventory_ids:
        errors.append("Refresh report must declare an explicit outcome for every registered source")
    if refresh.get("registered_source_count") != len(inventory_ids):
        errors.append("Refresh report registered source count differs from inventory")
    _validate_metric_registry(_read_json(Path(manifests_dir)/"metric_coverage.json"),inventory_ids,catalog,errors)

    _validate_dashboard_semantics(errors, warnings)
    _validate_deploy_docs_and_workflow(errors, warnings)

    return print_result(errors, warnings, fail_on_warning)


def print_result(errors: List[str], warnings: List[str], fail_on_warning: bool = False) -> int:
    if errors:
        print("Artifact validation failed with errors:")
        for item in errors:
            print(f"- ERROR: {item}")
    else:
        print("Artifact validation errors: none")

    if warnings:
        print("Artifact validation warnings:")
        for item in warnings:
            print(f"- WARNING: {item}")

    if errors or (fail_on_warning and warnings):
        return 1
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate generated research artifacts")
    parser.add_argument("--inventory", default="research/source_inventory.yaml")
    parser.add_argument("--catalog", default="data/manifests/catalog.json")
    parser.add_argument("--manifests", default="data/manifests")
    parser.add_argument("--fail-on-warning", action="store_true", default=False)
    args = parser.parse_args()

    raise SystemExit(run(args.inventory, args.catalog, args.manifests, args.fail_on_warning))


if __name__ == "__main__":
    main()
