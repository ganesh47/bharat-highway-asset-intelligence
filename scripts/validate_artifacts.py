#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List
import sys

import hashlib
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipelines.morth_appendix_validation import compare_appendix2_to_reference, validate_appendix2_snapshot


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


def _validate_entry(entry: Dict, manifest_root: Path, errors: List[str], warnings: List[str]) -> None:
    source_id = entry.get("source_id")
    if not source_id:
        errors.append("Catalog entry missing source_id")
        return

    manifest = manifest_root / f"{source_id}.json"
    if not manifest.exists():
        warnings.append(f"Missing per-source manifest: {source_id}")

    required_fields = ["source_id", "status", "metric_category", "source", "citations", "manifest", "overall_confidence_badge", "output_table_path"]
    for field in required_fields:
        if field not in entry:
            errors.append(f"Source {source_id} missing required catalog field: {field}")

    source_meta = entry.get("source", {})
    for field in ["publisher", "license_terms", "retrieved_at"]:
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

    if entry.get("metric_category") not in {"official_measured", "proxy_derived", "model_output"}:
        warnings.append(f"Source {source_id} has non-standard metric_category: {entry.get('metric_category')}")

    output_path = Path(entry.get("output_table_path")) if entry.get("output_table_path") else None
    if output_path and output_path.exists():
        output_size = output_path.stat().st_size
        if output_size <= 0:
            warnings.append(f"Source {source_id} output parquet is empty ({output_path})")
        if source_id == "data_gov_in_nhai_stateut_project_delay_status_2024":
            try:
                df = pd.read_parquet(output_path)
                state_col = "state/ut"
                project_col = "number_of_projects"
                delayed_col = "number_of_delayed_projects"
                required_cols = {state_col, project_col, delayed_col}
                missing_cols = required_cols - set(df.columns)
                if missing_cols:
                    errors.append(f"Source {source_id} missing required columns: {sorted(missing_cols)}")
                else:
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
                required_cols = {"states/ut", "fatalities_-_2020", "fatalities_-_2021", "fatalities_-_2022", "injuries_-_2020", "injuries_-_2021", "injuries_-_2022"}
                missing_cols = required_cols - set(df.columns)
                if missing_cols:
                    errors.append(f"Source {source_id} missing required columns: {sorted(missing_cols)}")
                else:
                    core = df.loc[~df["states/ut"].astype(str).str.strip().str.lower().isin({"total", "india", "all india"})].copy()
                    if core["states/ut"].nunique() < 35:
                        errors.append(f"Source {source_id} has insufficient state/UT coverage: {core['states/ut'].nunique()} unique states")
                    for col in ["fatalities_-_2020", "fatalities_-_2021", "fatalities_-_2022", "injuries_-_2020", "injuries_-_2021", "injuries_-_2022"]:
                        vals = pd.to_numeric(core[col], errors="coerce")
                        if vals.isna().all():
                            errors.append(f"Source {source_id} contains no usable numeric values in {col}")
                        elif vals.isna().any():
                            missing_states = set(core.loc[vals.isna(), "states/ut"].astype(str).str.strip())
                            allowed_missing_states = {"Ladakh"} if col.endswith("_2020") else set()
                            if missing_states - allowed_missing_states:
                                warnings.append(f"Source {source_id} contains partial missing values in {col}")
                        if (vals < 0).any():
                            errors.append(f"Source {source_id} contains negative values in {col}")
                    if core.duplicated(subset=["states/ut"]).any():
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
        catalog_ids.add(sid)
        _validate_entry(entry, Path(manifests_dir), errors, warnings)

    missing = sorted(inventory_ids - catalog_ids)
    for sid in missing:
        warnings.append(f"Inventory source missing from catalog: {sid}")

    for sid in sorted(catalog_ids - inventory_ids):
        if sid != "correlation_matrix":
            warnings.append(f"Catalog has non-inventory source: {sid}")

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
