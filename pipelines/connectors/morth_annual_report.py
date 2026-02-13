from __future__ import annotations

from typing import Any, Dict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .base import ConnectorResult, ConnectorSpec
from pipelines.common import ensure_dirs, sha256_for_file, write_json, write_parquet
from pipelines.quality import evaluate


class MoRTHAnnualReportConnector:
    spec = ConnectorSpec(
        name="morth_annual_report_pdf",
        version="0.1.0",
        source_ids=["morth_annual_report_pdf"],
        inputs=["source_inventory.source_item"],
        outputs=["parquet"],
        citation_mapping={
            "primary_source": "publisher_org+dataset_title",
            "permanent_identifier": "year+table",
            "license_terms": "license_terms",
            "anchor": "pdf_page",
        },
    )

    def run(self, source: Dict[str, Any], raw_root: Path, processed_root: Path, manifest_root: Path) -> ConnectorResult:
        source_id = source["source_id"]
        output_path = processed_root / f"{source_id}.parquet"
        manifest_path = manifest_root / f"{source_id}.json"
        ensure_dirs(raw_root.as_posix(), processed_root.as_posix(), manifest_root.as_posix())
        now = datetime.now(timezone.utc).isoformat()

        manual_csv = raw_root / "manual" / f"{source_id}.csv"
        if manual_csv.exists():
            try:
                df = pd.read_csv(manual_csv)
            except Exception as exc:
                return ConnectorResult(
                    source_id=source_id,
                    output_table_path=output_path,
                    manifest={
                        "source_id": source_id,
                        "status": "stubs_disabled",
                        "skip_reason": f"manual_csv_parse_failed:{exc}",
                        "metric_category": "official_measured",
                        "source": {
                            "publisher": source.get("publisher_org"),
                            "title": source.get("dataset_title"),
                            "url": source.get("url"),
                            "retrieved_at": now,
                            "license_terms": source.get("license_terms"),
                            "official_flag": source.get("official_flag", True),
                        },
                        "citations": {
                            "permanent_identifier": source.get("permanent_identifier_hint"),
                            "anchor": "annual_report_csv_parse_error",
                            "note": "Manual CSV exists but cannot be parsed. Re-run with corrected CSV format.",
                        },
                    },
                    skipped=True,
                    skip_reason="manual_csv_parse_failed",
                )

            if "source_type" not in df.columns:
                df["source_type"] = "official_measured"
            if "source_id" not in df.columns:
                df["source_id"] = source_id
            if "metric_category" not in df.columns:
                df["metric_category"] = "official_measured"
            if "dataset_source" not in df.columns:
                df["dataset_source"] = source.get("dataset_title")
            df["retrieved_at"] = now

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
                    "url": source.get("url"),
                    "retrieved_at": now,
                    "license_terms": source.get("license_terms"),
                    "official_flag": source.get("official_flag", True),
                },
                "citations": {
                    "permanent_identifier": source.get("permanent_identifier_hint"),
                    "anchor": "manual_annual_report_csv_page_reference",
                    "note": "Official annual-report table extracts imported from approved CSV snapshot.",
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
                "retrieved_at": now,
            }
            manifest.update(evaluate(df, source | manifest["source"]))
            write_json(manifest, manifest_path)
            return ConnectorResult(source_id=source_id, output_table_path=output_path, manifest=manifest)

        manual_pdf = raw_root / "manual" / f"{source_id}.pdf"
        if manual_pdf.exists():
            df = pd.DataFrame(
                [
                    {
                        "source_id": source_id,
                        "source_type": "official_measured",
                        "dataset_source": source.get("dataset_title"),
                        "metric_name": "annual_report_pdf_ingested",
                        "metric_value": 1,
                        "unit": "binary",
                        "retrieved_at": now,
                        "citation_anchor": "pdf_page_placeholder",
                    }
                ]
            )
        else:
            df = pd.DataFrame(
                [
                    {
                        "source_id": source_id,
                        "source_type": "official_measured",
                        "metric_category": "official_measured",
                        "dataset_source": source.get("dataset_title"),
                        "metric_name": "annual_report_pdf_ingestion_status",
                        "metric_value": 0,
                        "unit": "binary",
                        "retrieved_at": now,
                        "status": "stubs_disabled",
                        "note": "Official PDF not available in local manual drop.",
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
                    "skip_reason": "no_manual_pdf_found",
                    "metric_category": "official_measured",
                    "source": {
                        "publisher": source.get("publisher_org"),
                        "title": source.get("dataset_title"),
                        "url": source.get("url"),
                        "retrieved_at": now,
                        "license_terms": source.get("license_terms"),
                        "official_flag": source.get("official_flag", True),
                    },
                    "citations": {
                        "permanent_identifier": source.get("permanent_identifier_hint"),
                        "anchor": "manual_pdf_missing",
                        "note": "Ingestion disabled by approval gate. Add manual PDF under data/raw/manual.",
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
                skip_reason="no_manual_pdf",
            )

        write_parquet(df, output_path)
        manifest = {
            "source_id": source_id,
            "connector": self.spec.name,
            "version": self.spec.version,
            "status": "stub_parsed",
            "metric_category": "official_measured",
            "source": {
                "publisher": source.get("publisher_org"),
                "title": source.get("dataset_title"),
                "url": source.get("url"),
                "retrieved_at": now,
                "license_terms": source.get("license_terms"),
                "official_flag": source.get("official_flag", True),
            },
            "citations": {
                "permanent_identifier": "annual report + page",
                "anchor": "pdf_page_placeholder",
                "note": "Use PDF-to-table parser config per year table mapping before production use",
            },
            "manifest": {
                "raw_files": [
                    {
                        "path": str(manual_pdf),
                        "sha256": sha256_for_file(manual_pdf),
                        "size_bytes": manual_pdf.stat().st_size,
                    }
                ],
                "output_files": [
                    {
                        "path": str(output_path),
                        "sha256": sha256_for_file(output_path),
                    }
                ],
                "row_count": 1,
                "columns": list(df.columns),
            },
            "retrieved_at": now,
        }
        manifest.update(evaluate(df, source | manifest["source"]))
        write_json(manifest, manifest_path)
        return ConnectorResult(source_id=source_id, output_table_path=output_path, manifest=manifest)
