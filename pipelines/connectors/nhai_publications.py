from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from .base import ConnectorResult, ConnectorSpec
from pipelines.common import ensure_dirs, sha256_for_file, write_json, write_parquet
from pipelines.quality import evaluate

import pandas as pd


class NHAIPublicationConnector:
    spec = ConnectorSpec(
        name="nhai_publication_index",
        version="0.1.0",
        source_ids=["nhai_press_release_index"],
        inputs=["source_inventory.source_item"],
        outputs=["parquet"],
        citation_mapping={
            "primary_source": "publisher_org+dataset_title",
            "permanent_identifier": "url_slug",
            "anchor": "press-release page",
            "license_terms": "license_terms",
        },
    )

    def run(self, source: Dict[str, Any], raw_root: Path, processed_root: Path, manifest_root: Path) -> ConnectorResult:
        source_id = source["source_id"]
        output_path = processed_root / f"{source_id}.parquet"
        manifest_path = manifest_root / f"{source_id}.json"
        ensure_dirs(processed_root.as_posix(), manifest_root.as_posix())

        df = pd.DataFrame(
            [
                {
                    "source_id": source_id,
                    "source_type": "official_measured",
                    "dataset_source": source.get("dataset_title"),
                    "metric_name": "publication_index_metadata",
                    "metric_value": 1,
                    "unit": "binary",
                    "retrieved_at": datetime.now(timezone.utc).isoformat(),
                    "publisher": source.get("publisher_org"),
                    "retrieval_url": source.get("url"),
                    "metric_category": "official_measured",
                }
            ]
        )
        write_parquet(df, output_path)

        manifest = {
            "source_id": source_id,
            "connector": self.spec.name,
            "version": self.spec.version,
            "status": "metadata_only",
            "metric_category": "official_measured",
            "source": {
                "publisher": source.get("publisher_org"),
                "title": source.get("dataset_title"),
                "retrieved_at": datetime.now(timezone.utc).isoformat(),
                "license_terms": source.get("license_terms"),
                "official_flag": source.get("official_flag", True),
            },
            "citations": {
                "permanent_identifier": "nhai_press_release_index",
                "anchor": "index_page",
                "note": "Metadata-only connector. Full page content not fetched to avoid prohibited scraping.",
            },
            "manifest": {
                "raw_files": [],
                "output_files": [
                    {
                        "path": str(output_path),
                        "sha256": sha256_for_file(output_path),
                    }
                ],
                "row_count": 1,
                "columns": list(df.columns),
            },
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
        }
        manifest.update(evaluate(df, source | manifest["source"]))
        write_json(manifest, manifest_path)
        return ConnectorResult(source_id=source_id, output_table_path=output_path, manifest=manifest)
