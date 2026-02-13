#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List

import hashlib


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
