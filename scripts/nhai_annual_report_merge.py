from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

import nhai_annual_report_extractor as extractor


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _shard_manifest_paths(shards_root: Path) -> list[Path]:
    return sorted(shards_root.rglob("extraction_manifest.json"))


def _resolve_shard_output_path(manifest_path: Path, recorded_output_path: str) -> Path:
    recorded = Path(recorded_output_path)
    if recorded.exists():
        return recorded

    manifest_dir = manifest_path.parent
    candidates: list[Path] = [
        manifest_dir / recorded.name,
        manifest_dir / "yearly" / recorded.name,
    ]

    shard_part_index = next((idx for idx, part in enumerate(recorded.parts) if part.startswith("shard-")), None)
    if shard_part_index is not None:
        remainder = recorded.parts[shard_part_index + 1 :]
        if remainder:
            candidates.append(manifest_dir / Path(*remainder))

    yearly_index = next((idx for idx, part in enumerate(recorded.parts) if part == "yearly"), None)
    if yearly_index is not None:
        candidates.append(manifest_dir / Path(*recorded.parts[yearly_index:]))

    seen: set[Path] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        if candidate.exists():
            return candidate

    recursive_matches = sorted(manifest_dir.rglob(recorded.name))
    if len(recursive_matches) == 1:
        return recursive_matches[0]
    if recursive_matches:
        yearly_matches = [path for path in recursive_matches if "yearly" in path.parts]
        if len(yearly_matches) == 1:
            return yearly_matches[0]
        return yearly_matches[0] if yearly_matches else recursive_matches[0]

    raise SystemExit(
        f"Unable to resolve shard output file for manifest {manifest_path}: {recorded_output_path}"
    )


def _expected_shard_documents(source_parquet: Path, total_shards: int) -> dict[int, set[str]]:
    source_df = pd.read_parquet(source_parquet)
    annual_rows = extractor._filter_annual_rows(source_df)
    annual_rows = annual_rows.drop_duplicates(subset=["source_document_url", "financial_year", "document_title"]).copy()
    expected: dict[int, set[str]] = {}
    for shard_index in range(total_shards):
        shard_rows, document_keys = extractor._select_shard_rows(annual_rows, total_shards, shard_index)
        del shard_rows
        expected[shard_index] = {str(item) for item in document_keys}
    return expected


def _validate_shard_manifests(manifests: list[dict[str, Any]], source_parquet: Path, allow_incomplete: bool) -> dict[str, Any]:
    if not manifests:
        raise SystemExit("No shard manifests found.")

    totals = {int(m.get("shard", {}).get("total_shards", 0)) for m in manifests}
    if len(totals) != 1:
        raise SystemExit(f"Shard manifests disagree on total_shards: {sorted(totals)}")
    total_shards = totals.pop()
    shard_indices = sorted(int(m.get("shard", {}).get("shard_index", -1)) for m in manifests)
    if any(idx < 0 for idx in shard_indices):
        raise SystemExit("Shard manifest missing shard_index metadata.")

    expected = list(range(total_shards))
    missing = [idx for idx in expected if idx not in shard_indices]
    extra = [idx for idx in shard_indices if idx not in expected]
    if (missing or extra) and not allow_incomplete:
        raise SystemExit(f"Shard set incomplete. missing={missing} extra={extra}")

    parser_envs = [m.get("parser_environment", {}) for m in manifests]
    first_env = parser_envs[0]
    for env in parser_envs[1:]:
        if env != first_env:
            raise SystemExit("Shard manifests disagree on parser_environment.")

    seen_keys: dict[str, int] = {}
    duplicate_keys: list[str] = []
    for manifest in manifests:
        shard_index = int(manifest.get("shard", {}).get("shard_index", -1))
        for key in manifest.get("shard", {}).get("document_keys", []):
            if key in seen_keys:
                duplicate_keys.append(key)
            else:
                seen_keys[key] = shard_index
    if duplicate_keys:
        raise SystemExit(f"Duplicate document keys found across shards: {len(duplicate_keys)}")

    expected_documents = _expected_shard_documents(source_parquet, total_shards)
    for manifest in manifests:
        shard_meta = manifest.get("shard", {})
        shard_index = int(shard_meta.get("shard_index", -1))
        observed = {str(item) for item in shard_meta.get("document_keys", [])}
        expected = expected_documents.get(shard_index, set())
        if observed != expected:
            raise SystemExit(
                f"Shard {shard_index} document coverage mismatch. observed={len(observed)} expected={len(expected)}"
            )

    return {
        "total_shards": total_shards,
        "shard_indices": shard_indices,
        "missing_shards": missing,
        "extra_shards": extra,
        "document_key_count": len(seen_keys),
        "parser_environment": first_env,
    }


def _sort_source_documents(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[Any, ...], dict[str, Any]] = {}
    for item in items:
        key = (
            int(item.get("doc_index", 0)),
            str(item.get("source_document_url", "")),
            str(item.get("source_document_title", "")),
        )
        deduped[key] = item
    return sorted(deduped.values(), key=lambda item: (int(item.get("doc_index", 0)), str(item.get("source_document_url", ""))))


def _merge_yearly_datasets(manifests: list[dict[str, Any]], output_root: Path) -> tuple[dict[str, Any], list[pd.DataFrame]]:
    yearly_root = output_root / "yearly"
    extractor._safe_path(yearly_root)

    by_year: dict[str, list[pd.DataFrame]] = defaultdict(list)
    by_year_sources: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for manifest in manifests:
        shard_index = int(manifest.get("shard", {}).get("shard_index", -1))
        manifest_path = Path(str(manifest["__manifest_path"]))
        for year, payload in manifest.get("yearly_datasets", {}).items():
            output_path = _resolve_shard_output_path(manifest_path, str(payload["output_path"]))
            df = extractor._coerce_frame(pd.read_parquet(output_path), default_file=output_path)
            by_year[year].append(df)
            source_documents = payload.get("source_documents") or []
            if not source_documents and (payload.get("source_document_url") or payload.get("source_document_title")):
                source_documents = [
                    {
                        "doc_index": -1,
                        "source_document_url": payload.get("source_document_url", ""),
                        "source_document_title": payload.get("source_document_title", ""),
                        "error": None,
                    }
                ]
            for item in source_documents:
                source_item = dict(item)
                source_item.setdefault("shard_index", shard_index)
                by_year_sources[year].append(source_item)

    yearly_manifest: dict[str, Any] = {}
    all_frames: list[pd.DataFrame] = []
    for year in sorted(by_year.keys(), key=extractor._coerce_year):
        df = pd.concat(by_year[year], ignore_index=True) if by_year[year] else pd.DataFrame(columns=extractor.CANONICAL_COLUMNS)
        df = extractor._coerce_frame(df)
        df = df.drop_duplicates(subset=extractor.CANONICAL_COLUMNS, keep="first")
        df = extractor._sort_output_frame(df)
        df["row_index"] = range(len(df))
        out_path = yearly_root / f"nhai_annual_report_{year}.parquet"
        df["lineage_output_file"] = str(out_path)
        df.to_parquet(out_path, index=False)
        docs = _sort_source_documents(by_year_sources[year])
        yearly_manifest[year] = {
            "source_document_url": docs[0]["source_document_url"] if len(docs) == 1 else "",
            "source_document_title": docs[0]["source_document_title"] if len(docs) == 1 else "",
            "source_documents": docs,
            "document_count": len(docs),
            "output_path": str(out_path),
            "rows": int(len(df)),
            "schema": [str(c) for c in df.columns],
            "first_records": int((df["record_type"] == "table_row").sum()),
            "method_mix": df["extraction_method"].value_counts(dropna=False).to_dict(),
            "quality": int((pd.to_numeric(df["extraction_confidence"], errors="coerce").fillna(0) < 0.45).sum()),
        }
        all_frames.append(df)

    return yearly_manifest, all_frames


def main() -> None:
    parser = argparse.ArgumentParser(description="Deterministically merge sharded NHAI annual report OCR outputs")
    parser.add_argument("--source-parquet", default="data/processed/nhai_annual_report_documents.parquet")
    parser.add_argument("--shards-root", default="data/processed/nhai_annual_report_tables/shards")
    parser.add_argument("--output-root", default="data/processed/nhai_annual_report_tables")
    parser.add_argument("--canonical-output", default="data/processed/nhai_annual_report_tables_canonical.parquet")
    parser.add_argument("--quality-report-output", default="data/processed/nhai_annual_report_tables/quality_report.json")
    parser.add_argument("--allow-incomplete", action="store_true")
    args = parser.parse_args()

    source_parquet = Path(args.source_parquet)
    shards_root = Path(args.shards_root)
    output_root = Path(args.output_root)
    canonical_output = Path(args.canonical_output)
    quality_output = Path(args.quality_report_output)

    manifest_paths = _shard_manifest_paths(shards_root)
    manifests = [{**_load_json(path), "__manifest_path": str(path)} for path in manifest_paths]
    validation = _validate_shard_manifests(manifests, source_parquet, allow_incomplete=args.allow_incomplete)

    yearly_manifest, all_frames = _merge_yearly_datasets(manifests, output_root)
    canonical_df, canonical_summary = extractor.build_canonical(output_root, canonical_output)
    if not canonical_df.empty:
        canonical_df = extractor._coerce_frame(canonical_df)
        canonical_df = extractor._sort_output_frame(canonical_df)

    all_df = pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame(columns=extractor.CANONICAL_COLUMNS)
    all_df = extractor._coerce_frame(all_df)
    all_df = all_df.drop_duplicates(subset=extractor.CANONICAL_COLUMNS, keep="first")
    all_df = extractor._sort_output_frame(all_df)
    quality = extractor.build_quality_report(
        all_df,
        canonical_summary,
        yearly_manifest,
        quality_output,
        validation["parser_environment"],
    )

    parser_metrics: dict[str, int] = defaultdict(int)
    parallel_errors: list[dict[str, Any]] = []
    for manifest in manifests:
        for key, value in manifest.get("parser_metrics", {}).items():
            parser_metrics[str(key)] += int(value)
        parallel_errors.extend(list(manifest.get("parallel_errors", [])))

    merged_manifest = {
        "source_parquet": manifests[0].get("source_parquet", ""),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rows_input": validation["document_key_count"],
        "parser_environment": validation["parser_environment"],
        "parallel_workers": sum(int(m.get("parallel_workers", 1)) for m in manifests),
        "parallel_order_strategy": "doc_index_modulo_total_shards",
        "parallel_errors": parallel_errors,
        "yearly_datasets": yearly_manifest,
        "parser_metrics": dict(parser_metrics),
        "canonical": canonical_summary,
        "quality": quality["quality"],
        "quality_report": str(quality_output),
        "rows_merged": int(len(canonical_df)) if not canonical_df.empty else 0,
        "shard": {
            "merged": True,
            "shards_root": str(shards_root),
            "total_shards": validation["total_shards"],
            "shard_indices": validation["shard_indices"],
            "missing_shards": validation["missing_shards"],
            "extra_shards": validation["extra_shards"],
            "document_key_count": validation["document_key_count"],
        },
        "merge_validation": {
            "status": "ok" if not validation["missing_shards"] and not validation["extra_shards"] else "incomplete",
            "shard_manifests": [str(path) for path in manifest_paths],
            "year_count": len(yearly_manifest),
            "schema_reference": canonical_summary.get("schema_compatibility", {}).get("reference_shape", []),
        },
    }
    extractor._write_json(output_root / "extraction_manifest.json", merged_manifest)

    print(f"Merged shard manifests: {len(manifest_paths)} from {shards_root}")
    print(f"Yearly datasets written to: {output_root / 'yearly'}")
    print(f"Canonical dataset: {canonical_output}")
    print(f"Quality report: {quality_output}")
    print(f"Manifest: {output_root / 'extraction_manifest.json'}")


if __name__ == "__main__":
    main()
