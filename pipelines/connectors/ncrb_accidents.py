from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from .base import ConnectorResult, ConnectorSpec
from pipelines.common import ensure_dirs, sha256_for_file, write_json, write_parquet
from pipelines.quality import evaluate


class NCRBAccidentsConnector:
    spec = ConnectorSpec(
        name="ncrb_accidents_pdf",
        version="0.1.0",
        source_ids=["ncrb_road_accidents_state_year"],
        inputs=["source_inventory.source_item"],
        outputs=["parquet"],
        citation_mapping={
            "primary_source": "publisher_org+dataset_title",
            "permanent_identifier": "year+table",
            "anchor": "pdf_page",
            "license_terms": "license_terms",
        },
    )

    def run(self, source: Dict[str, Any], raw_root: Path, processed_root: Path, manifest_root: Path) -> ConnectorResult:
        source_id = source["source_id"]
        output_path = processed_root / f"{source_id}.parquet"
        manifest_path = manifest_root / f"{source_id}.json"
        ensure_dirs(raw_root.as_posix(), processed_root.as_posix(), manifest_root.as_posix())

        manual_csv = raw_root / "manual" / f"{source_id}.csv"
        if not manual_csv.exists():
            df = pd.DataFrame(
                [
                    {
                        "source_id": source_id,
                        "source_type": "official_measured",
                        "metric_category": "official_measured",
                        "dataset_source": source.get("dataset_title"),
                        "metric_name": "road_accidents_state_year",
                        "metric_value": 0,
                        "unit": "binary",
                        "retrieved_at": datetime.now(timezone.utc).isoformat(),
                        "status": "stubs_disabled",
                        "notes": "No approved manual CSV available. Add official PDF-derived CSV to data/raw/manual.",
                    }
                ]
            )
            write_parquet(df, output_path)

            return ConnectorResult(
                source_id=source_id,
                output_table_path=output_path,
                manifest={
                    "source_id": source_id,
                    "status": "stubs_disabled",
                    "skip_reason": "no_manual_csv_found",
                    "metric_category": "official_measured",
                    "source": {
                        "publisher": source.get("publisher_org"),
                        "title": source.get("dataset_title"),
                        "url": source.get("url"),
                        "retrieved_at": datetime.now(timezone.utc).isoformat(),
                        "license_terms": source.get("license_terms"),
                        "official_flag": source.get("official_flag", True),
                    },
                    "citations": {
                        "permanent_identifier": source.get("permanent_identifier_hint"),
                        "anchor": "manual_csv_missing",
                        "note": "Ingestion paused; requires official report-derived table upload.",
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
                        "row_count": 1,
                        "columns": list(df.columns),
                    },
                },
                skipped=True,
                skip_reason="no_manual_csv",
            )

        df = pd.read_csv(manual_csv)
        if "source_type" not in df.columns:
            df["source_type"] = "official_measured"
        if "source_id" not in df.columns:
            df["source_id"] = source_id
        df["retrieved_at"] = datetime.now(timezone.utc).isoformat()

        write_parquet(df, output_path)

        manifest = {
            "source_id": source_id,
            "connector": self.spec.name,
            "version": self.spec.version,
            "status": "manual_ingest",
            "metric_category": "official_measured",
            "source": {
                "publisher": source.get("publisher_org"),
                "title": source.get("dataset_title"),
                "retrieved_at": datetime.now(timezone.utc).isoformat(),
                "license_terms": source.get("license_terms"),
                "official_flag": source.get("official_flag", True),
            },
            "citations": {
                "permanent_identifier": source.get("permanent_identifier_hint"),
                "anchor": "official_pdf_tables_or_manual_csv",
            },
            "manifest": {
                "raw_files": [
                    {
                        "path": str(manual_csv),
                        "sha256": sha256_for_file(manual_csv),
                        "size_bytes": manual_csv.stat().st_size,
                    }
                ],
                "output_files": [
                    {
                        "path": str(output_path),
                        "sha256": sha256_for_file(output_path),
                    }
                ],
                "row_count": int(len(df)),
                "columns": list(df.columns),
            },
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
        }
        manifest.update(evaluate(df, source | manifest["source"]))
        write_json(manifest, manifest_path)

        return ConnectorResult(source_id=source_id, output_table_path=output_path, manifest=manifest)
