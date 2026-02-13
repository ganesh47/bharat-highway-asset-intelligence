from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import requests

from .base import ConnectorResult, ConnectorSpec
from pipelines.common import ensure_dirs, sha256_for_file, write_json, write_parquet
from pipelines.quality import evaluate
from pipelines.connectors.stub_connectors import _read_manual_csv


class RBIMOSPIMacroConnector:
    spec = ConnectorSpec(
        name="rbi_mospi_macro_pull",
        version="0.1.0",
        source_ids=["rbi_mospi_macro_indicators"],
        inputs=["source_inventory.source_item"],
        outputs=["parquet"],
        citation_mapping={
            "primary_source": "publisher_org+dataset_title",
            "permanent_identifier": "indicator_code+release",
            "anchor": "table",
            "license_terms": "license_terms",
        },
    )

    def run(self, source: Dict[str, Any], raw_root: Path, processed_root: Path, manifest_root: Path) -> ConnectorResult:
        source_id = source["source_id"]
        output_path = processed_root / f"{source_id}.parquet"
        manifest_path = manifest_root / f"{source_id}.json"
        ensure_dirs(raw_root.as_posix(), processed_root.as_posix(), manifest_root.as_posix())

        manual_df, manual_csv = _read_manual_csv(source_id, raw_root)
        if manual_df is not None:
            if "source_type" not in manual_df.columns:
                manual_df["source_type"] = "official_measured"
            if "source_id" not in manual_df.columns:
                manual_df["source_id"] = source_id
            if "metric_category" not in manual_df.columns:
                manual_df["metric_category"] = "official_measured"
            if "dataset_source" not in manual_df.columns:
                manual_df["dataset_source"] = source.get("dataset_title")
            manual_df["retrieved_at"] = datetime.now(timezone.utc).isoformat()

            df = manual_df.copy(deep=True)
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
                    "retrieved_at": datetime.now(timezone.utc).isoformat(),
                    "license_terms": source.get("license_terms"),
                    "official_flag": source.get("official_flag", True),
                },
                "citations": {
                    "permanent_identifier": "official indicator code + release month",
                    "anchor": "official_manual_macro_snapshot",
                    "note": "Macro indicators imported through approved manual artifact.",
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

        if not source.get("allow_auto_fetch", False):
            df = pd.DataFrame(
                [
                    {
                        "source_id": source_id,
                        "source_type": "official_measured",
                        "metric_category": "official_measured",
                        "dataset_source": source.get("dataset_title"),
                        "metric_name": "macro_indicator_pull",
                        "metric_value": 0,
                        "unit": "binary",
                        "retrieved_at": datetime.now(timezone.utc).isoformat(),
                        "status": "disabled_by_policy",
                        "note": "Auto-fetch off by approval gate in source inventory.",
                    }
                ]
            )
            write_parquet(df, output_path)
            return ConnectorResult(
                source_id=source_id,
                output_table_path=output_path,
                manifest={
                    "source_id": source_id,
                    "status": "disabled",
                    "skip_reason": "allow_auto_fetch_false",
                    "metric_category": "official_measured",
                    "source": {
                        "publisher": source.get("publisher_org"),
                        "title": source.get("dataset_title"),
                        "url": source.get("url"),
                        "retrieved_at": datetime.now(timezone.utc).isoformat(),
                        "license_terms": source.get("license_terms"),
                        "official_flag": source.get("official_flag", True),
                    },
                    "notes": "Open endpoint is not yet validated. Keep disabled by default for manual-approval gate compliance.",
                    "citations": {
                        "permanent_identifier": source.get("permanent_identifier_hint"),
                        "anchor": "source_gate_disabled",
                        "note": "Endpoint unvalidated. Enable only after approved endpoint is documented.",
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
                skip_reason="disabled",
            )

        endpoint = source.get("url", "")
        try:
            resp = requests.get(endpoint, timeout=45)
            resp.raise_for_status()
            raw_path = raw_root / source_id / f"raw_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            raw_path.write_text(resp.text, encoding="utf-8")
            df = pd.read_csv(raw_path)
        except Exception as exc:
            return ConnectorResult(
                source_id=source_id,
                output_table_path=output_path,
                manifest={
                    "source_id": source_id,
                    "status": "skipped",
                    "skip_reason": f"endpoint_not_open_or_not_supported_in_v1:{exc}",
                    "metric_category": "official_measured",
                    "source": {
                        "publisher": source.get("publisher_org"),
                        "official_flag": source.get("official_flag", True),
                    },
                    "notes": "This stub remains ready for a documented public endpoint. Keep source disabled until endpoint is validated.",
                },
                skipped=True,
                skip_reason="endpoint_failed",
            )

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
            "status": "automated",
            "metric_category": "official_measured",
            "source": {
                "publisher": source.get("publisher_org"),
                "title": source.get("dataset_title"),
                "url": endpoint,
                "retrieved_at": datetime.now(timezone.utc).isoformat(),
                "license_terms": source.get("license_terms"),
                "official_flag": source.get("official_flag", True),
            },
            "citations": {
                "permanent_identifier": "indicator_code+table",
                "anchor": "official_endpoint",
            },
            "manifest": {
                "raw_files": [
                    {
                        "path": str(raw_path),
                        "sha256": sha256_for_file(raw_path),
                        "size_bytes": raw_path.stat().st_size,
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
