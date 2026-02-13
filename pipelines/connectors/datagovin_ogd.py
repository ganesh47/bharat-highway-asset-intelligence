from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict

import os
import pandas as pd
import requests

from .base import ConnectorResult, ConnectorSpec
from pipelines.common import ensure_dirs, sha256_for_file, write_json, write_parquet
from pipelines.quality import evaluate


class DataGovInConnector:
    spec = ConnectorSpec(
        name="data_gov_in_ogd",
        version="0.1.0",
        source_ids=["data_gov_in_nhai_projects_api", "data_gov_in_nhai_project_finance_api"],
        inputs=["source_inventory.source_item"],
        outputs=["parquet"],
        citation_mapping={
            "primary_source": "publisher_org+dataset_title",
            "retrieval_date": "retrieved_at",
            "permanent_identifier": "resource_id",
            "license_terms": "license_terms",
            "anchor": "api_path_or_table",
        },
    )

    def _manual_file(self, source_id: str, raw_root: Path) -> tuple[pd.DataFrame | None, Path | None]:
        base = raw_root / "manual"
        for name in (f"{source_id}.csv", f"{source_id}.json", f"{source_id}.xlsx"):
            path = base / name
            if not path.exists():
                continue
            if path.suffix == ".csv":
                return pd.read_csv(path), path
            if path.suffix == ".json":
                return pd.read_json(path), path
            if path.suffix == ".xlsx":
                return pd.read_excel(path), path
        return None, None

    def run(self, source: Dict[str, Any], raw_root: Path, processed_root: Path, manifest_root: Path) -> ConnectorResult:
        source_id = source["source_id"]
        output_path = processed_root / f"{source_id}.parquet"
        manifest_path = manifest_root / f"{source_id}.json"
        ensure_dirs(raw_root.as_posix(), processed_root.as_posix(), manifest_root.as_posix())

        if source.get("auth") in {"restricted", "captcha"}:
            return ConnectorResult(
                source_id=source_id,
                output_table_path=output_path,
                manifest={
                    "source_id": source_id,
                    "status": "skipped",
                    "skip_reason": "auth_restriction",
                    "metric_category": "official_measured",
                    "source": {
                        "publisher": source.get("publisher_org"),
                        "official_flag": source.get("official_flag", True),
                    },
                },
                skipped=True,
                skip_reason="auth_restriction",
            )

        resource_id = source.get("resource_id") or os.getenv(source.get("resource_id_env", "").strip(), None)
        api_key = os.getenv(source.get("api_key_env", ""), None)

        manual_df, manual_path = self._manual_file(source_id, raw_root)
        raw_path: Path | None = None
        anchor = "manual_upload"

        # Automatic fetch path for official data.gov.in API
        if source.get("allow_auto_fetch") and resource_id and api_key:
            api_url = source.get("url", "").format(resource_id=resource_id)
            try:
                params = {
                    "api-key": api_key,
                    "format": "json",
                    "limit": 10000,
                }
                resp = requests.get(api_url, params=params, timeout=60)
                resp.raise_for_status()
                raw_path = raw_root / source_id / f"raw_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                raw_path.write_text(resp.text, encoding="utf-8")

                payload = resp.json()
                records = payload.get("records") if isinstance(payload, dict) else None
                if isinstance(records, list):
                    df = pd.DataFrame(records)
                else:
                    raise ValueError("Unexpected API response shape. Expected dict with records list.")

                anchor = f"api:{source_id}:records"
            except Exception as exc:
                if manual_df is None:
                    return ConnectorResult(
                        source_id=source_id,
                        output_table_path=output_path,
                        manifest={
                            "source_id": source_id,
                            "status": "failed",
                            "skip_reason": f"auto_fetch_failed:{exc}",
                            "metric_category": "official_measured",
                            "source": {
                                "publisher": source.get("publisher_org"),
                                "official_flag": source.get("official_flag", True),
                            },
                        },
                        skipped=True,
                        skip_reason=f"auto_fetch_failed:{exc}",
                    )
                df = manual_df.copy()
                raw_path = manual_path
                anchor = "manual_upload"
        elif manual_df is not None:
            df = manual_df
            raw_path = manual_path
            anchor = "manual_upload"
        else:
            return ConnectorResult(
                source_id=source_id,
                output_table_path=output_path,
                manifest={
                    "source_id": source_id,
                    "status": "skipped",
                    "skip_reason": "no_source_data_available",
                    "metric_category": "official_measured",
                    "source": {
                        "publisher": source.get("publisher_org"),
                        "official_flag": source.get("official_flag", True),
                    },
                },
                skipped=True,
                skip_reason="no_source_data_available",
            )

        df = df.copy(deep=True)
        if "source_type" not in df.columns:
            df["source_type"] = "official_measured"
        if "source_id" not in df.columns:
            df["source_id"] = source_id
        df["dataset_source"] = source.get("dataset_title")
        df["retrieved_at"] = datetime.now(timezone.utc).isoformat()

        if df.empty:
            df = pd.DataFrame()

        write_parquet(df, output_path)

        manifest: Dict[str, Any] = {
            "source_id": source_id,
            "connector": self.spec.name,
            "version": self.spec.version,
            "status": "ok" if raw_path else "fallback",
            "metric_category": "official_measured",
            "source": {
                "publisher": source.get("publisher_org"),
                "title": source.get("dataset_title"),
                "domain": source.get("domain"),
                "url": source.get("url"),
                "retrieval_method": source.get("retrieval_method"),
                "access_type": source.get("access_type"),
                "retrieved_at": df["retrieved_at"].iloc[0] if not df.empty else datetime.now(timezone.utc).isoformat(),
                "license_terms": source.get("license_terms"),
                "official_flag": source.get("official_flag", True),
            },
            "citations": {
                "permanent_identifier": source.get("permanent_identifier_hint")
                or source.get("resource_id")
                or source.get("resource_id_env"),
                "anchor": anchor,
            },
            "manifest": {
                "raw_files": [
                    {
                        "path": str(raw_path),
                        "sha256": sha256_for_file(raw_path),
                        "size_bytes": raw_path.stat().st_size,
                    }
                ]
                if raw_path
                else [],
                "output_files": [
                    {
                        "path": str(output_path),
                        "format": "parquet",
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
