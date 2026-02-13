from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict
import pandas as pd

from research.loader import load_inventory
from pipelines.connectors import CONNECTORS
from pipelines.common import ensure_dirs, write_catalog, write_json, read_json
from pipelines.quality import evaluate


def find_connector_for_source(source_id: str):
    for connector in CONNECTORS:
        if source_id in connector.spec.source_ids:
            return connector
    return None


def run_ingestion(
    inventory_path: str = "research/source_inventory.yaml",
    selected_sources: list[str] | None = None,
    raw_root: Path = Path("data/raw"),
    processed_root: Path = Path("data/processed"),
    manifest_root: Path = Path("data/manifests"),
    catalog_path: Path = Path("data/manifests/catalog.json"),
) -> Dict[str, dict]:
    inv = load_inventory(inventory_path)
    source_map = {s["source_id"]: s for s in inv.sources}

    target_sources = list(source_map)
    if selected_sources:
        target_sources = [sid for sid in selected_sources if sid in source_map]

    ensure_dirs(raw_root.as_posix(), processed_root.as_posix(), manifest_root.as_posix(), catalog_path.parent.as_posix())

    existing = read_json(catalog_path).get("datasets", []) if catalog_path.exists() else []
    catalog_entries = list(existing)

    for source_id in target_sources:
        source = source_map[source_id]
        connector = find_connector_for_source(source_id)
        if connector is None:
            manifest = {
                "source_id": source_id,
                "status": "not_mapped",
                "notes": "No connector declared for this source_id",
                "source": {
                    "official_flag": source.get("official_flag"),
                },
            }
            catalog_entries = [item for item in catalog_entries if item.get("source_id") != source_id] + [manifest]
            continue

        result = connector.run(source, raw_root, processed_root, manifest_root)
        manifest_entry = result.manifest
        if "source" not in manifest_entry:
            manifest_entry["source"] = {}
        manifest_entry.setdefault("source", {}).setdefault("official_flag", source.get("official_flag", False))
        manifest_entry.setdefault("output_table_path", str(result.output_table_path))
        score_df = pd.DataFrame()
        if Path(manifest_entry["output_table_path"]).exists():
            try:
                score_df = pd.read_parquet(manifest_entry["output_table_path"])
            except Exception:
                score_df = pd.DataFrame()

        score_ctx = source | manifest_entry.get("source", {})
        if manifest_entry.get("status"):
            score_ctx["status"] = manifest_entry.get("status")
        score_ctx["update_frequency"] = source.get("update_frequency")
        manifest_entry.update(evaluate(score_df, score_ctx))
        write_json(manifest_entry, manifest_root / f"{source_id}.json")
        catalog_entries = [
            item for item in catalog_entries if item.get("source_id") != manifest_entry.get("source_id")
        ] + [manifest_entry]

    write_catalog(catalog_path, catalog_entries)
    return {entry.get("source_id"): entry for entry in catalog_entries}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory", default="research/source_inventory.yaml")
    parser.add_argument("--source", action="append", help="run only specified source_ids")
    args = parser.parse_args()

    run_ingestion(args.inventory, args.source)
    print("Ingestion complete. Catalog written to data/manifests/catalog.json")


if __name__ == "__main__":
    main()
