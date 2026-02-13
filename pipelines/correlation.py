from __future__ import annotations

from itertools import combinations
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

from pipelines.common import (
    ensure_dirs,
    read_json,
    sha256_for_file,
    write_catalog,
    write_json,
    write_parquet,
)
from pipelines.quality import evaluate


def _safe_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


def _catalog_path(path: str = "data/manifests/catalog.json") -> List[Dict[str, Any]]:
    payload = read_json(Path(path))
    return payload.get("datasets", [])


def _metric_columns(df: pd.DataFrame, key_col: str) -> List[str]:
    numeric = []
    for col in df.columns:
        if col == key_col or col == "year":
            continue
        if col in {"source_type", "source_id", "metric_category", "dataset_source", "unit", "metric_name", "retrieved_at", "publisher", "retrieval_url", "notes", "note", "status", "metric_category"}:
            continue
        if pd.api.types.is_numeric_dtype(df[col]):
            numeric.append(col)
            continue
        casted = pd.to_numeric(df[col], errors="coerce")
        if not casted.isna().all():
            df[col] = casted
            numeric.append(col)
    return numeric


def _build_metric_long(source_id: str, df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    dimension_col = None
    for candidate in ["state", "district", "corridor", "project_id", "region", "state_name", "project_name"]:
        if candidate in df.columns:
            dimension_col = candidate
            break

    if dimension_col is None:
        return pd.DataFrame()
    if "year" not in df.columns:
        return pd.DataFrame()

    rows: List[Dict[str, Any]] = []
    if "metric_name" in df.columns and "metric_value" in df.columns:
        source_metric_df = df[[dimension_col, "year", "metric_name", "metric_value"]].copy()
        source_metric_df["metric_name"] = source_metric_df["metric_name"].astype(str).str.strip()
        source_metric_df = source_metric_df.dropna(subset=["metric_name"])
        source_metric_df["metric_key"] = source_metric_df["metric_name"].map(
            lambda name: f"{source_id}.{str(name).replace(' ', '_').lower()}"
        )
        source_metric_df = source_metric_df.rename(columns={"metric_value": "value", dimension_col: "dimension_value"})
        rows = source_metric_df.to_dict("records")
    else:
        numeric_cols = _metric_columns(df.copy(), dimension_col)
        for col in numeric_cols:
            if col not in df.columns:
                continue
            subset = df[[dimension_col, "year", col]].copy()
            subset = subset.rename(columns={dimension_col: "dimension_value", col: "value"})
            if subset["value"].isna().all():
                continue
            subset["metric_key"] = f"{source_id}.{col}"
            rows.extend(subset.to_dict("records"))

    if not rows:
        return pd.DataFrame()

    long = pd.DataFrame(rows)
    long["dimension_value"] = long["dimension_value"].astype(str)
    long["year"] = pd.to_numeric(long["year"], errors="coerce")
    long = long.dropna(subset=["year"])
    long["year"] = long["year"].astype(int)
    long["value"] = pd.to_numeric(long["value"], errors="coerce")
    long = long.dropna(subset=["value"])

    return long


def _widen_metrics(long_frames: List[Tuple[pd.DataFrame, str]]) -> Tuple[pd.DataFrame, Dict[str, Dict[str, Any]]]:
    merged = None
    metadata: Dict[str, Dict[str, Any]] = {}

    for frame, source_id in long_frames:
        if frame.empty:
            continue
        pivot = (
            frame.pivot_table(
                index=["dimension_value", "year"],
                columns="metric_key",
                values="value",
                aggfunc="mean",
            )
            .reset_index()
            .rename(columns={"dimension_value": "entity"})
        )
        if merged is None:
            merged = pivot
        else:
            merged = pd.merge(merged, pivot, on=["entity", "year"], how="outer")

        if frame["metric_key"].nunique():
            for metric_key in frame["metric_key"].dropna().unique():
                metadata[str(metric_key)] = source_id

    if merged is None:
        return pd.DataFrame(), {}
    return merged, metadata


def _pairwise_corr(
    wide_df: pd.DataFrame,
    metric_metadata: Dict[str, str],
    min_overlap: int,
) -> pd.DataFrame:
    if wide_df.empty:
        return pd.DataFrame()

    metric_cols = [c for c in wide_df.columns if c not in {"entity", "year"}]
    if len(metric_cols) < 2:
        return pd.DataFrame()

    rows = []
    for col_a, col_b in combinations(metric_cols, 2):
        pair = wide_df[["year", col_a, col_b]].dropna()
        if len(pair) < min_overlap:
            continue
        if pair[col_a].nunique(dropna=True) <= 1 or pair[col_b].nunique(dropna=True) <= 1:
            continue
        corr = pair[col_a].corr(pair[col_b], method="pearson")
        if pd.isna(corr):
            continue
        rows.append(
            {
                "metric_a": col_a,
                "metric_b": col_b,
                "source_a": metric_metadata.get(col_a),
                "source_b": metric_metadata.get(col_b),
                "correlation": float(round(corr, 6)),
                "overlap_records": int(len(pair)),
                "year_min": int(pair["year"].min()) if len(pair) else None,
                "year_max": int(pair["year"].max()) if len(pair) else None,
                "created_at": _safe_now(),
                "method": "pearson_pairwise_state_year",
            }
        )

    return pd.DataFrame(rows)


def run_correlation(
    catalog_path: str = "data/manifests/catalog.json",
    output_path: Path = Path("data/processed/correlation_matrix.parquet"),
    manifest_root: str = "data/manifests",
    catalog_out_path: str = "data/manifests/catalog.json",
    min_overlap: int = 2,
) -> Dict[str, Any]:
    entries = _catalog_path(catalog_path)
    if not entries:
        return {"status": "skipped", "reason": "catalog_missing"}

    long_frames: List[Tuple[pd.DataFrame, str]] = []
    for entry in entries:
        source_id = entry.get("source_id")
        if not source_id:
            continue
        if entry.get("status") in {"disabled", "stubs_disabled"} and str(entry.get("skip_reason", "")).startswith("manual"):
            continue
        metric_cat = str(entry.get("metric_category", "official_measured"))
        if metric_cat == "model_output":
            continue
        path = entry.get("output_table_path") or f"data/processed/{source_id}.parquet"
        parquet = Path(path)
        if not parquet.exists():
            continue

        try:
            df = pd.read_parquet(parquet)
        except Exception:
            continue
        if df.empty:
            continue

        long = _build_metric_long(source_id, df)
        if long.empty:
            continue
        long_frames.append((long, source_id))

    wide, metric_meta = _widen_metrics(long_frames)
    if wide.empty:
        ensure_dirs(str(output_path.parent), manifest_root)
        empty = pd.DataFrame()
        write_parquet(empty, output_path)
        return {"status": "skipped", "reason": "no_joinable_metrics", "output": str(output_path)}

    corr = _pairwise_corr(wide, metric_meta, min_overlap=min_overlap)
    if corr.empty:
        ensure_dirs(str(output_path.parent), manifest_root)
        write_parquet(corr, output_path)
    else:
        ensure_dirs(str(output_path.parent), manifest_root)
        write_parquet(corr, output_path)

    manifest_path = Path(manifest_root) / "correlation_matrix.json"
    manifest = {
        "source_id": "correlation_matrix",
        "connector": "correlation_scoring",
        "version": "0.1.0",
        "status": "generated",
        "output_table_path": str(output_path),
        "metric_category": "model_output",
        "source": {
            "publisher": "Bharat Highway internal correlation engine",
            "title": "Model-derived pairwise correlation matrix (state/year join)",
            "retrieved_at": _safe_now(),
            "official_flag": False,
            "license_terms": "Model output for analysis only. Not an official policy statement.",
        },
        "citations": {
            "permanent_identifier": "correlation_matrix_v1",
            "anchor": "derived_pairwise_pearson_state_year",
            "note": "Derived from available numeric metrics across official measured and approved proxy inputs.",
        },
        "manifest": {
            "raw_files": [],
            "output_files": [
                {
                    "path": str(output_path),
                    "format": "parquet",
                    "sha256": sha256_for_file(output_path),
                }
            ],
            "row_count": int(len(corr)),
            "columns": list(corr.columns),
        },
        "retrieved_at": _safe_now(),
    }
    manifest.update(evaluate(corr, manifest["source"]))
    write_json(manifest, manifest_path)

    existing = read_json(Path(catalog_out_path)).get("datasets", [])
    datasets = [d for d in existing if d.get("source_id") != "correlation_matrix"]
    datasets.append(manifest)
    write_catalog(Path(catalog_out_path), datasets)

    return {
        "status": "done",
        "source_id": "correlation_matrix",
        "rows": int(len(corr)),
        "output": str(output_path),
    }


def main() -> None:
    result = run_correlation()
    print(result)


if __name__ == "__main__":
    main()
