from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import requests

from .base import ConnectorResult, ConnectorSpec
from pipelines.common import ensure_dirs, sha256_for_file, write_json, write_parquet
from pipelines.quality import evaluate


API_RECORD_KEYS = ("records", "data", "result")


SOURCE_FILE_KEYS = [
    "field_datafile",
    "field_datafile_private",
    "field_datafile_url",
]


class DataGovInConnector:
    spec = ConnectorSpec(
        name="data_gov_in_ogd",
        version="0.2.0",
        source_ids=[
            "data_gov_in_nhai_projects_api",
            "data_gov_in_nhai_project_finance_api",
            "data_gov_in_nhai_state_projects_api",
        ],
        inputs=["source_inventory.source_item"],
        outputs=["parquet"],
        citation_mapping={
            "primary_source": "publisher_org+dataset_title",
            "retrieval_date": "retrieved_at",
            "permanent_identifier": "resource_id",
            "license_terms": "license_terms",
            "anchor": "api_or_resource_page",
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

    @staticmethod
    def _decode_payload_value(value: str) -> str:
        if not value:
            return value
        # Data.gov.in payload often escapes slashes with \u002F.
        decoded = value.encode("utf-8").decode("unicode_escape")
        return decoded.replace("\\u002F", "/")

    def _extract_file_candidates(self, html: str) -> list[str]:
        values: list[str] = []
        for key in SOURCE_FILE_KEYS:
            # Try both raw and escaped JSON-string-like payload shapes.
            for pattern in (f'{key}:"([^"]+)"', f'{key}\\":\\"([^\\\"]+)\\"'):
                import re

                match = re.search(pattern, html)
                if match:
                    values.append(self._decode_payload_value(match.group(1)))

        candidates: list[str] = []
        for value in values:
            if not value:
                continue
            value = value.strip()
            # Avoid returning duplicated or malformed links.
            if not value or value in {"#", "null", "undefined"}:
                continue
            if value.startswith("http://") or value.startswith("https://"):
                candidates.append(value)
                # Prefer mirror host variants for data.gov.in endpoints that intermittently require it.
                if "https://www.data.gov.in/" in value:
                    candidates.append(value.replace("https://www.data.gov.in", "https://data.gov.in"))
            elif value.startswith("/"):
                candidates.append(f"https://www.data.gov.in{value}")
            else:
                candidates.append(f"https://www.data.gov.in/{value}")

            # Public object URLs often reject direct reads; canonical site copy usually works.
            if "files/ogdpv2dms/s3fs-public/" in value and "/sites/default/files/" not in value:
                candidates.append(f"https://www.data.gov.in/sites/default/files/{value.rsplit('/', 1)[-1]}")
                candidates.append(f"https://data.gov.in/sites/default/files/{value.rsplit('/', 1)[-1]}")

            if "/system/files/" in value and "/sites/default/files/" not in value:
                candidates.append(f"https://www.data.gov.in{value.replace('/system/files/', '/sites/default/files/')}")
            if "https://www.data.gov.in/" in value and "/files/ogdpv2dms/" in value:
                candidates.append(value.replace("https://www.data.gov.in/files/", "https://data.gov.in/sites/default/files/"))
            if "/ogdpv2dms/" in value and ".csv" in value.lower():
                filename = value.rsplit("/", 1)[-1]
                if filename:
                    candidates.append(f"https://data.gov.in/sites/default/files/{filename}")

        # De-duplicate, preserving first-seen order.
        unique: list[str] = []
        for url in candidates:
            if url not in unique:
                unique.append(url)
        return unique

    @staticmethod
    def _parse_api_records(payload: Any) -> pd.DataFrame:
        if isinstance(payload, dict):
            for key in API_RECORD_KEYS:
                if isinstance(payload.get(key), list):
                    return pd.DataFrame(payload[key])

        if isinstance(payload, list):
            return pd.DataFrame(payload)

        raise ValueError("Unexpected API payload shape.")

    @staticmethod
    def _standardize_df(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy(deep=True)
        out.columns = [
            str(col)
            .strip()
            .replace("\n", " ")
            .replace("  ", " ")
            for col in out.columns
        ]
        out.columns = [
            col.lower().replace(" ", "_").replace("(", "").replace(")", "").replace("%", "pct")
            for col in out.columns
        ]
        return out

    @staticmethod
    def _parse_year(df: pd.DataFrame) -> pd.DataFrame:
        for col in list(df.columns):
            if col == "year":
                try:
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                except Exception:
                    # Keep raw string if conversion fails; consistency checks will handle missingness.
                    pass
                break
        return df

    @staticmethod
    def _write_raw_response(
        raw_root: Path,
        source_id: str,
        payload: str | bytes,
        extension: str,
    ) -> Path:
        raw_root.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        raw_path = raw_root / source_id / f"raw_{ts}{extension}"
        raw_path.parent.mkdir(parents=True, exist_ok=True)

        mode = "wb" if isinstance(payload, bytes) else "w"
        kwargs: Dict[str, Any] = {"encoding": "utf-8"} if mode == "w" else {}
        with raw_path.open(mode, **kwargs) as handle:
            handle.write(payload)
        return raw_path

    @staticmethod
    def _api_headers(source: Dict[str, Any]) -> Dict[str, str]:
        return {
            "accept": "application/json",
            "user-agent": "BHAI-data-connector/0.2 (+official-first)",
        }

    def run(
        self,
        source: Dict[str, Any],
        raw_root: Path,
        processed_root: Path,
        manifest_root: Path,
    ) -> ConnectorResult:
        source_id = source["source_id"]
        output_path = processed_root / f"{source_id}.parquet"
        manifest_path = manifest_root / f"{source_id}.json"

        now = datetime.now(timezone.utc).isoformat()
        ensure_dirs(raw_root.as_posix(), processed_root.as_posix(), manifest_root.as_posix())

        manual_df, manual_path = self._manual_file(source_id, raw_root)
        raw_path: Optional[Path] = None
        anchor = "manual_upload"
        status = "candidate_ready"
        skip_reason = None

        if source.get("auth") in {"restricted", "captcha"}:
            return ConnectorResult(
                source_id=source_id,
                output_table_path=output_path,
                manifest={
                    "source_id": source_id,
                    "status": "disabled",
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

        df: Optional[pd.DataFrame] = None

        # 1) API path (official JSON API)
        resource_id = source.get("resource_id") or source.get("resource_id_env")
        if isinstance(resource_id, str) and resource_id.startswith("PLACEHOLDER_"):
            resource_id = None
        if not resource_id and source.get("resource_id_env"):
            resource_id = os.getenv(source.get("resource_id_env", "").strip(), None)

        api_url = source.get("url", "") if source.get("allow_auto_fetch") and source.get("access_type") == "API" else ""
        api_key = source.get("api_key_env")
        if api_url and resource_id and api_key:
            api_url = api_url.format(resource_id=resource_id)

        if api_url:
            params = {
                "api-key": os.getenv(api_key, ""),
                "format": "json",
                "limit": 50000,
            }
            try:
                response = requests.get(api_url, params=params, timeout=60, headers=self._api_headers(source))
                response.raise_for_status()
                raw_path = self._write_raw_response(raw_root / source_id, source_id, response.text, ".json")
                df = self._parse_api_records(response.json())
                df = self._standardize_df(df)
                status = "automated"
                anchor = f"api:{source_id}:{resource_id}"
            except Exception as exc:  # pragma: no cover - network dependent
                skip_reason = f"api_fetch_failed:{exc}"

        # 2) Official resource page fallback (official file link extraction)
        if df is None and source.get("allow_auto_fetch") and source.get("resource_page_url"):
            resource_url = source["resource_page_url"]
            try:
                page_resp = requests.get(resource_url, timeout=45, headers={"user-agent": "BHAI-research-scan/0.2"})
                page_resp.raise_for_status()
                candidates = self._extract_file_candidates(page_resp.text)

                for candidate in candidates:
                    candidate_resp = requests.get(candidate, timeout=20, headers={"user-agent": "BHAI-research-scan/0.2"})
                    if not candidate_resp.ok:
                        continue

                    content_type = (candidate_resp.headers.get("Content-Type") or "").lower()
                    guessed_ext = candidate.lower().rsplit(".", 1)[-1]

                    if "json" in content_type or guessed_ext == "json":
                        temp = self._write_raw_response(
                            raw_root / source_id,
                            source_id,
                            candidate_resp.text,
                            ".json",
                        )
                        response_payload = candidate_resp.json()
                        df = self._parse_api_records(response_payload)
                    elif "csv" in content_type or guessed_ext == "csv":
                        temp = self._write_raw_response(
                            raw_root / source_id,
                            source_id,
                            candidate_resp.content,
                            ".csv",
                        )
                        df = pd.read_csv(temp)
                    elif "excel" in content_type or guessed_ext in {"xls", "xlsx"}:
                        temp = self._write_raw_response(
                            raw_root / source_id,
                            source_id,
                            candidate_resp.content,
                            ".xlsx",
                        )
                        df = pd.read_excel(temp)
                    elif "text" in content_type and guessed_ext in {"txt", "tsv"}:
                        temp = self._write_raw_response(
                            raw_root / source_id,
                            source_id,
                            candidate_resp.text,
                            ".csv",
                        )
                        df = pd.read_csv(temp)
                    else:
                        # Probe raw content as CSV as a tolerant fallback for small official attachments.
                        temp = self._write_raw_response(
                            raw_root / source_id,
                            source_id,
                            candidate_resp.content,
                            ".csv",
                        )
                        try:
                            df = pd.read_csv(temp)
                        except Exception:
                            df = None

                    if df is not None:
                        raw_path = temp
                        status = "ok"
                        anchor = f"resource_page_file:{candidate}"
                        break

                if df is None:
                    raise RuntimeError("No downloadable official table/file link was discoverable from resource page metadata.")
            except Exception as exc:  # pragma: no cover - network dependent
                skip_reason = f"official_page_fetch_failed:{exc}"
                df = None

        # 3) Manual fallback (only if no approved API/page result)
        if df is None and manual_df is not None:
            df = manual_df.copy(deep=True)
            raw_path = manual_path
            status = "manual_ingest"
            anchor = "manual_upload"

        if df is None:
            return ConnectorResult(
                source_id=source_id,
                output_table_path=output_path,
                manifest={
                    "source_id": source_id,
                    "status": "failed",
                    "skip_reason": skip_reason or "no_source_data_available",
                    "metric_category": "official_measured",
                    "source": {
                        "publisher": source.get("publisher_org"),
                        "official_flag": source.get("official_flag", True),
                    },
                },
                skipped=True,
                skip_reason=skip_reason or "no_source_data_available",
            )

        df = self._standardize_df(df)
        df = self._parse_year(df)

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

        source_meta = {
            "publisher": source.get("publisher_org"),
            "title": source.get("dataset_title"),
            "domain": source.get("domain"),
            "url": source.get("resource_page_url") or source.get("url"),
            "retrieval_method": source.get("retrieval_method"),
            "access_type": source.get("access_type"),
            "retrieved_at": now,
            "license_terms": source.get("license_terms"),
            "official_flag": source.get("official_flag", True),
        }

        raw_files: list[dict] = []
        if raw_path and raw_path.exists():
            raw_files.append(
                {
                    "path": str(raw_path),
                    "sha256": sha256_for_file(raw_path),
                    "size_bytes": raw_path.stat().st_size,
                }
            )

        manifest = {
            "source_id": source_id,
            "connector": self.spec.name,
            "version": self.spec.version,
            "status": status,
            "metric_category": "official_measured",
            "source": source_meta,
            "citations": {
                "permanent_identifier": (
                    source.get("permanent_identifier_hint") or source.get("resource_id") or source.get("resource_id_env")
                ),
                "anchor": anchor,
                "note": "Official file discovered from data.gov.in resource metadata and parsed with deterministic connector logic.",
            },
            "manifest": {
                "raw_files": raw_files,
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
            "retrieved_at": now,
        }

        if skip_reason:
            manifest["skip_reason"] = skip_reason

        manifest.update(evaluate(df, source | source_meta))
        write_json(manifest, manifest_path)
        return ConnectorResult(source_id=source_id, output_table_path=output_path, manifest=manifest)
