from __future__ import annotations

import argparse
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from pipelines.common import ensure_dirs, read_json, sha256_for_file, write_catalog, write_json, write_parquet
from pipelines.quality import evaluate

MIN_OVERLAP = 10
SAFETY_SOURCE = "data_gov_in_nh_fatalities_injuries_state_year"
DELAY_SOURCE = "data_gov_in_nhai_stateut_project_delay_status_2024"
APPROVED_PAIRS = [
    {"source_a": SAFETY_SOURCE, "metric_a": "fatalities", "source_b": SAFETY_SOURCE, "metric_b": "injuries", "reason": "Same reported NH/expressway safety universe and calendar year"},
    {"source_a": DELAY_SOURCE, "metric_a": "number_of_projects", "source_b": DELAY_SOURCE, "metric_b": "number_of_delayed_projects", "reason": "Same NH project snapshot and State/UT universe"},
]
JOIN_KEYS = ["entity_type", "entity_id", "period", "period_basis", "road_class", "agency_scope", "statement_basis", "estimate_type"]
CORRELATION_COLUMNS = ["metric_a", "metric_b", "source_a", "source_b", "correlation", "spearman", "overlap_records", "entity_count", "period_count", "coverage_a", "coverage_b", "year_min", "year_max", "created_at", "method", "scope", "interpretation"]
STATE_ALIASES = {
    "orissa": "odisha", "uttaranchal": "uttarakhand", "uttrakhand": "uttarakhand",
    "pondicherry": "puducherry", "nct of delhi": "delhi", "delhi (ut)": "delhi",
    "andaman & nicobar islands": "andaman and nicobar islands",
    "a & n islands": "andaman and nicobar islands", "a&n islands": "andaman and nicobar islands",
    "dadra & nagar haveli & daman & diu": "dadra and nagar haveli and daman and diu",
    "dadra and nagar haveli & daman and diu": "dadra and nagar haveli and daman and diu",
}


def canonical_entity(value: str) -> str:
    label = re.sub(r"\s+", " ", str(value).strip().lower())
    return STATE_ALIASES.get(label, label)


def _safe_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_metric_long(source_id: str, df: pd.DataFrame, entry: dict | None = None) -> pd.DataFrame:
    """Only semantically declared measures enter the correlation input."""
    rows = []
    if {"entity_id", "entity_type", "metric", "value", "period_basis", "road_class", "agency", "statement_basis", "estimate_type", "unit"} <= set(df.columns):
        selected = df.loc[df["analytical_eligible"].fillna(False).eq(True)] if "analytical_eligible" in df else df.iloc[0:0]
        for row in selected.to_dict("records"):
            if row.get("evidence_class") in {"forecast", "valuation", "model_output", "unverified"}:
                continue
            rows.append({"entity_type": row["entity_type"], "entity_id": row["entity_id"],
                         "period": f"{row.get('period_start', '')}/{row.get('period_end', '')}",
                         "period_basis": row["period_basis"], "road_class": row["road_class"],
                         "agency_scope": row["agency"], "statement_basis": row["statement_basis"],
                         "estimate_type": row["estimate_type"], "unit": row["unit"],
                         "metric_key": row["metric"], "value": row["value"], "source_id": source_id})
    elif source_id in {SAFETY_SOURCE, DELAY_SOURCE}:
        state_col = next((c for c in ["states/ut", "state/ut", "state", "state_ut"] if c in df), None)
        if state_col is None:
            return pd.DataFrame()
        for row in df.to_dict("records"):
            state = canonical_entity(row[state_col])
            if state in {"india", "all india", "total", "grand total"}:
                continue
            base = {"entity_type": "state_ut", "entity_id": state, "road_class": "National Highway including expressways",
                    "agency_scope": "MoRTH NH safety" if source_id == SAFETY_SOURCE else "MoRTH NH project snapshot",
                    "statement_basis": "reported", "estimate_type": "actual", "unit": "count", "source_id": source_id}
            if source_id == SAFETY_SOURCE:
                for col, value in row.items():
                    match = re.search(r"^(fatalities|injuries).*?((?:19|20)\d{2})$", col)
                    if match:
                        rows.append(base | {"metric_key": match.group(1), "value": value, "period": match.group(2), "period_basis": "calendar_year"})
            else:
                for metric in ["number_of_projects", "number_of_delayed_projects"]:
                    if metric in row:
                        rows.append(base | {"metric_key": metric, "value": row[metric], "period": (entry or {}).get("source_as_of_date") or "2024-03-31", "period_basis": "snapshot"})
    long = pd.DataFrame(rows)
    if not long.empty:
        long["value"] = pd.to_numeric(long["value"], errors="coerce")
        long = long.dropna(subset=["value"])
    return long


def _approved_correlations(long: pd.DataFrame, pairs: list[dict], min_overlap: int = MIN_OVERLAP) -> tuple[pd.DataFrame, list[dict]]:
    rows, diagnostics = [], []
    min_overlap = max(MIN_OVERLAP, min_overlap)
    for spec in pairs:
        diagnostic = {"source_a": spec["source_a"], "metric_a": spec["metric_a"], "source_b": spec["source_b"], "metric_b": spec["metric_b"], "approval_reason": spec.get("reason"), "minimum_overlap": min_overlap}
        if long.empty:
            diagnostics.append(diagnostic | {"status": "unavailable", "overlap_records": 0})
            continue
        a = long.loc[long["source_id"].eq(spec["source_a"]) & long["metric_key"].eq(spec["metric_a"])].copy()
        b = long.loc[long["source_id"].eq(spec["source_b"]) & long["metric_key"].eq(spec["metric_b"])].copy()
        if a.empty or b.empty:
            diagnostics.append(diagnostic | {"status": "missing_metric", "overlap_records": 0})
            continue
        if a.duplicated(JOIN_KEYS).any() or b.duplicated(JOIN_KEYS).any():
            diagnostics.append(diagnostic | {"status": "duplicate_entity_period", "overlap_records": 0})
            continue
        # Unit semantics must be stable per metric, even when the two metrics have different units.
        if a["unit"].nunique() != 1 or b["unit"].nunique() != 1:
            diagnostics.append(diagnostic | {"status": "mixed_units", "overlap_records": 0})
            continue
        merged = a[JOIN_KEYS + ["value"]].merge(b[JOIN_KEYS + ["value"]], on=JOIN_KEYS, suffixes=("_a", "_b"), validate="one_to_one")
        n = len(merged)
        diagnostic.update(overlap_records=n, available_a=len(a), available_b=len(b), coverage_a=round(n / len(a), 4), coverage_b=round(n / len(b), 4), unit_a=a["unit"].iloc[0], unit_b=b["unit"].iloc[0])
        if n < min_overlap:
            diagnostics.append(diagnostic | {"status": "insufficient_compatible_overlap"})
            continue
        if merged["value_a"].nunique() < 2 or merged["value_b"].nunique() < 2:
            diagnostics.append(diagnostic | {"status": "constant_metric"})
            continue
        pearson = merged["value_a"].corr(merged["value_b"])
        spearman = merged["value_a"].rank().corr(merged["value_b"].rank())
        years = [int(y) for period in merged["period"] for y in re.findall(r"(?:19|20)\d{2}", str(period))]
        rows.append({"metric_a": f"{spec['source_a']}.{spec['metric_a']}", "metric_b": f"{spec['source_b']}.{spec['metric_b']}",
                     "source_a": spec["source_a"], "source_b": spec["source_b"], "correlation": round(float(pearson), 6), "spearman": round(float(spearman), 6),
                     "overlap_records": n, "entity_count": merged["entity_id"].nunique(), "period_count": merged["period"].nunique(),
                     "coverage_a": diagnostic["coverage_a"], "coverage_b": diagnostic["coverage_b"], "year_min": min(years) if years else None,
                     "year_max": max(years) if years else None, "created_at": _safe_now(), "method": "approved_pair_pearson_spearman",
                     "scope": "; ".join(sorted(merged["agency_scope"].unique())), "interpretation": "Descriptive association across compatible observations; not a causal or financial forecast"})
        diagnostics.append(diagnostic | {"status": "published"})
    return pd.DataFrame(rows, columns=CORRELATION_COLUMNS), diagnostics


def run_correlation(catalog_path: str = "data/manifests/catalog.json", output_path: Path = Path("data/processed/correlation_matrix.parquet"),
                    manifest_root: str = "data/manifests", catalog_out_path: str = "data/manifests/catalog.json", min_overlap: int = MIN_OVERLAP,
                    approved_pairs: list[dict] | None = None) -> dict[str, Any]:
    entries = read_json(Path(catalog_path)).get("datasets", [])
    frames, exclusions = [], []
    pairs = approved_pairs if approved_pairs is not None else APPROVED_PAIRS
    needed = {spec[key] for spec in pairs for key in ("source_a", "source_b")}
    for entry in entries:
        sid = entry.get("source_id")
        if sid not in needed:
            continue
        if entry.get("analytical_ready") is not True or entry.get("evidence_status") not in {"verified", "validated", "validated_primary", "validated_curated"} or entry.get("metric_category") not in {"official_measured", "issuer_disclosed"}:
            exclusions.append({"source_id": sid, "reason": "Unverified, synthetic, proxy or analytically unready source"})
            continue
        try:
            frame = _build_metric_long(sid, pd.read_parquet(entry["output_table_path"]), entry)
            if not frame.empty:
                frames.append(frame)
        except (OSError, ValueError, KeyError) as exc:
            exclusions.append({"source_id": sid, "reason": str(exc)})
    long = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    corr, diagnostics = _approved_correlations(long, pairs, min_overlap)
    ensure_dirs(output_path.parent, manifest_root)
    write_parquet(corr, output_path)
    report = {"generated_at": _safe_now(), "minimum_overlap": max(MIN_OVERLAP, min_overlap), "approved_pairs": pairs,
              "pairs": diagnostics, "excluded_sources": exclusions, "published_pair_count": len(corr),
              "notes": "No automatic entity matching, duplicate averaging, causal inference or synthetic inputs."}
    diagnostic_path = Path(manifest_root) / "correlation_diagnostics.json"
    write_json(report, diagnostic_path)
    now = _safe_now()
    manifest = {
        "source_id": "correlation_matrix", "connector": "correlation_scoring", "version": "0.2.0", "status": "generated",
        "output_table_path": str(output_path), "metric_category": "model_output", "evidence_status": "derived_analysis",
        "analytical_ready": False, "disclosure_ready": False, "extraction_status": "validated", "refresh_outcome": "model_generated", "last_checked_at": now,
        "source": {"publisher": "Bharat Highway correlation engine", "title": "Approved descriptive comparisons",
                   "retrieved_at": now, "official_flag": False, "license_terms": "Derived descriptive analysis; use input source licences."},
        "citations": {"permanent_identifier": "correlation_matrix_v2", "anchor": str(diagnostic_path),
                      "note": "Approved compatible input pairs with overlap and coverage diagnostics; no causal claim."},
        "derivation_inputs": [{"source_id": entry["source_id"], "data_checksum": entry.get("data_checksum"), "output_files": entry.get("manifest", {}).get("output_files", [])} for entry in entries if entry.get("source_id") in needed and entry.get("analytical_ready")],
        "diagnostics_path": str(diagnostic_path),
        "manifest": {"raw_files": [], "output_files": [{"path": str(output_path), "format": "parquet", "sha256": sha256_for_file(output_path)}], "row_count": len(corr), "columns": list(corr.columns)},
        "retrieved_at": now,
    }
    manifest.update(evaluate(corr, manifest | manifest["source"]))
    write_json(manifest, Path(manifest_root) / "correlation_matrix.json")
    current = read_json(Path(catalog_out_path)).get("datasets", [])
    write_catalog(Path(catalog_out_path), [d for d in current if d.get("source_id") != "correlation_matrix"] + [manifest])
    return {"status": "done", "source_id": "correlation_matrix", "rows": len(corr), "output": str(output_path), "diagnostics": str(diagnostic_path)}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-overlap", type=int, default=MIN_OVERLAP)
    args = parser.parse_args()
    print(run_correlation(min_overlap=args.min_overlap))


if __name__ == "__main__":
    main()
