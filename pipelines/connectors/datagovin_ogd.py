from __future__ import annotations

import os
from urllib.parse import urlparse
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
            "data_gov_in_nhai_projects_district_target_2023",
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
    def _safe_url_list(value: Any) -> list[str]:
        if not value:
            return []
        if isinstance(value, str):
            return [value]
        if isinstance(value, (list, tuple, set)):
            return [str(item) for item in value if item]
        return []

    @staticmethod
    def _normalize_resource_url(url: str) -> str:
        if not url:
            return ""
        return str(url).strip().replace(" ", "%20")

    def _collect_resource_file_urls(self, source: Dict[str, Any], page_html: str | None = None) -> list[str]:
        candidates: list[str] = []
        candidates.extend(self._safe_url_list(source.get("resource_file_urls")))

        # Prefer explicit file URLs from inventory first; only use page-derived candidates
        # when no explicit file URL is provided by curation.
        if not candidates and page_html:
            candidates.extend(self._extract_file_candidates(page_html))

        normalized: list[str] = []
        for value in candidates:
            if not value:
                continue
            normalized.append(self._normalize_resource_url(str(value)))
        # Preserve first-seen order and remove duplicates.
        out: list[str] = []
        for item in normalized:
            if item not in out:
                out.append(item)
        return out

    def _read_file_candidate(self, url: str, raw_root: Path, source_id: str) -> tuple[pd.DataFrame | None, Path | None]:
        response = requests.get(url, timeout=45, headers={"user-agent": "BHAI-research-scan/0.2"})
        if not response.ok:
            return None, None

        content_type = (response.headers.get("Content-Type") or "").lower()
        path_extension = ".csv"

        if "json" in content_type:
            try:
                payload = response.json()
                extension = ".json"
                raw_path = self._write_raw_response(raw_root / source_id, source_id, response.text, extension)
                return self._parse_api_records(payload), raw_path
            except Exception:
                # Fallback to text parsing for semi-CSV content mislabeled as JSON.
                pass

        guessed_ext = Path(urlparse(url).path).suffix.lower()
        if guessed_ext in {".json"}:
            path_extension = ".json"
        elif guessed_ext in {".xlsx", ".xls"}:
            path_extension = guessed_ext
        elif guessed_ext in {".txt", ".tsv"}:
            path_extension = ".txt"
        elif guessed_ext in {".csv"}:
            path_extension = ".csv"
        elif guessed_ext:
            path_extension = guessed_ext

        if path_extension == ".json":
            raw_path = self._write_raw_response(raw_root / source_id, source_id, response.text, ".json")
            try:
                return self._parse_api_records(response.json()), raw_path
            except Exception:
                return None, None
        if path_extension in {".xls", ".xlsx"}:
            raw_path = self._write_raw_response(raw_root / source_id, source_id, response.content, path_extension)
            try:
                return pd.read_excel(raw_path), raw_path
            except Exception:
                return None, None

        raw_path = self._write_raw_response(raw_root / source_id, source_id, response.content, ".csv")
        try:
            return pd.read_csv(raw_path), raw_path
        except Exception:
            # Final tolerant fallback for small official files that may be TSV or plain text.
            try:
                return pd.read_csv(raw_path, sep="\t"), raw_path
            except Exception:
                return None, None

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
    def _fetch_api_pages(api_url: str, base_params: Dict[str, Any], headers: Dict[str, str]) -> pd.DataFrame:
        limit = int(base_params.get("limit", 5000))
        offset = 0
        pages: list[pd.DataFrame] = []
        visited = 0
        while len(pages) < 200:
            query = dict(base_params)
            query["offset"] = offset
            response = requests.get(api_url, params=query, timeout=60, headers=headers)
            response.raise_for_status()

            payload = response.json()
            page_df = DataGovInConnector._parse_api_records(payload)
            if page_df.empty:
                break

            pages.append(page_df)
            visited += len(page_df)

            total = payload.get("total")
            count = payload.get("count") or payload.get("records_count")
            if isinstance(total, int) and visited >= total:
                break
            if isinstance(count, int):
                if count < limit:
                    break
            elif len(page_df) < limit:
                break
            if total is None and len(page_df) < limit:
                break
            offset += limit
            if offset > 200000:
                break

        if not pages:
            return pd.DataFrame()
        return pd.concat(pages, ignore_index=True)

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
        raw_paths: list[Path] = []
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

        df_frames: list[pd.DataFrame] = []
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
                "limit": 5000,
            }
            raw_path: Path | None = None
            try:
                api_df = self._fetch_api_pages(api_url, params, self._api_headers(source))
                if not api_df.empty:
                    raw_path = self._write_raw_response(raw_root / source_id, source_id, api_df.to_json(orient="records"), ".json")
                if not api_df.empty:
                    if raw_path is not None:
                        raw_paths.append(raw_path)
                    df_frames.append(api_df)
                status = "automated"
                anchor = f"api:{source_id}:{resource_id}"
            except Exception as exc:  # pragma: no cover - network dependent
                skip_reason = f"api_fetch_failed:{exc}"

        if len(df_frames) == 0 and source.get("allow_auto_fetch") and (
            source.get("resource_page_url") or source.get("resource_file_urls")
        ):
            resource_url = source.get("resource_page_url")
            page_status_issue = None
            try:
                page_resp = requests.get(
                    resource_url,
                    timeout=45,
                    headers={"user-agent": "BHAI-research-scan/0.2"},
                ) if resource_url else None
                page_html = ""
                if page_resp is not None:
                    page_html = page_resp.text if page_resp.status_code < 400 else ""
                    if page_resp.status_code >= 400:
                        page_status_issue = f"resource_page_http_{page_resp.status_code}"

                candidates = self._collect_resource_file_urls(source, page_html)
                if not candidates:
                    candidates = self._extract_file_candidates(page_html)

                anchors = []
                parse_failures = []
                seen_candidate_paths: set[str] = set()
                for candidate in candidates:
                    candidate_path = urlparse(candidate).path.rstrip("/").lower()
                    if candidate_path in seen_candidate_paths:
                        continue
                    seen_candidate_paths.add(candidate_path)

                    try:
                        parsed_df, parsed_path = self._read_file_candidate(candidate, raw_root, source_id)
                    except Exception as exc:
                        parse_failures.append(f"{candidate}|{exc.__class__.__name__}")
                        continue

                    if parsed_df is None:
                        parse_failures.append(candidate)
                        continue

                    if parsed_df.empty:
                        parse_failures.append(f"{candidate}|empty")
                        continue

                    df_frames.append(parsed_df)
                    if parsed_path is not None:
                        raw_paths.append(parsed_path)
                    status = "ok"
                    anchors.append(candidate)
                    # Stop at first usable official payload; additional mirrors are optional and
                    # often duplicated / unstable.
                    break

                if not df_frames:
                    raise RuntimeError("No downloadable official table/file link was discoverable from resource page metadata.")
                if anchors:
                    anchor = "resource_file:" + anchors[0]
                    if len(anchors) > 1:
                        anchor += f" (+{len(anchors)-1} more file(s))"
                    skip_reason = None
                elif page_status_issue and not parse_failures:
                    skip_reason = page_status_issue
                elif parse_failures and not anchors:
                    skip_reason = "resource_file_fetch_failed:" + " | ".join(parse_failures[:3])
            except Exception as exc:  # pragma: no cover - network dependent
                if not df_frames:
                    skip_reason = f"official_page_fetch_failed:{exc}"
                    df_frames = []

        # 3) Manual fallback (only if no approved API/page result)
        allow_manual_fallback = bool(source.get("manual_fallback", True))
        if len(df_frames) == 0 and manual_df is not None and allow_manual_fallback:
            df = manual_df.copy(deep=True)
            raw_paths = [manual_path] if manual_path else []
            status = "manual_ingest"
            anchor = "manual_upload"
            df_frames = [df]

        if not df_frames:
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

        df = pd.concat(df_frames, ignore_index=True)
        if not df.empty:
            # Avoid duplicated rows when a source exposes repeated file mirrors.
            df = df.drop_duplicates(ignore_index=True)
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
        seen_raw_paths = set()
        unique_raw_paths: list[Path] = []
        for raw_file in raw_paths:
            if raw_file is None or str(raw_file) in seen_raw_paths:
                continue
            seen_raw_paths.add(str(raw_file))
            unique_raw_paths.append(raw_file)

        for raw_file in unique_raw_paths:
            if raw_file is None or not raw_file.exists():
                continue
            raw_files.append(
                {
                    "path": str(raw_file),
                    "sha256": sha256_for_file(raw_file),
                    "size_bytes": raw_file.stat().st_size,
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
