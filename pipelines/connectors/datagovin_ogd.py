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
from pipelines.url_safety import collect_allowed_hosts_from_source, sanitize_public_http_url


API_RECORD_KEYS = ("records", "data", "result")
DATA_GOV_ALLOWED_HOST_SUFFIXES = {"data.gov.in"}


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
            "data_gov_in_nhai_projects_completed_undercon_awarded_last3yrs",
            "data_gov_in_nhai_statewise_nh_project_status_2024_25",
            "data_gov_in_nhai_stateut_project_delay_status_2024",
            "data_gov_in_nhai_tamil_nh_major_ongoing_2024_2026",
            "data_gov_in_nhai_himachal_nhai_projects_ongoing",
            "data_gov_in_nhai_punjab_42_projects_implementation",
            "data_gov_in_nhai_yearwise_nh_constructed_2014_15",
            "data_gov_in_nhai_stateut_length_constructed_2019_24",
            "data_gov_in_nhai_district_projects_implemented",
            "data_gov_in_road_accidents_nhs_2003_2016",
            "data_gov_in_road_accidents_india_2003_2016",
            "data_gov_in_road_fatal_accidents_2003_2016",
            "data_gov_in_nh_fatalities_injuries_state_year",
            "data_gov_in_gsdp_stateut_current_prices_2017_23",
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
        def is_data_gov_host(candidate: str) -> bool:
            host = (urlparse(candidate).hostname or "").lower()
            return host == "www.data.gov.in" or host == "data.gov.in"

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
                if is_data_gov_host(value) and (urlparse(value).hostname or "").lower() == "www.data.gov.in":
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
            if is_data_gov_host(value) and "/files/ogdpv2dms/" in urlparse(value).path:
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

    def _read_file_candidate(
        self,
        url: str,
        raw_root: Path,
        source_id: str,
        allowed_hosts: set[str],
    ) -> tuple[pd.DataFrame | None, Path | None]:
        safe_url = sanitize_public_http_url(
            url,
            allowed_hosts=allowed_hosts,
            allowed_host_suffixes=DATA_GOV_ALLOWED_HOST_SUFFIXES,
        )
        if not safe_url:
            return None, None
        response = requests.get(safe_url, timeout=45, headers={"user-agent": "BHAI-research-scan/0.2"})
        final_url = sanitize_public_http_url(
            response.url or safe_url,
            allowed_hosts=allowed_hosts,
            allowed_host_suffixes=DATA_GOV_ALLOWED_HOST_SUFFIXES,
        )
        if not final_url:
            return None, None
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

        guessed_ext = Path(urlparse(final_url).path).suffix.lower()
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
        encodings = ("utf-8-sig", "utf-8", "cp1252", "latin1")
        last_error: Exception | None = None
        for encoding in encodings:
            try:
                return pd.read_csv(raw_path, encoding=encoding), raw_path
            except Exception as exc:
                last_error = exc
                continue

        # Final tolerant fallback for small official files that may be TSV or plain text.
        for encoding in encodings:
            try:
                return pd.read_csv(raw_path, sep="\t", encoding=encoding), raw_path
            except Exception:
                pass

        # Keep a lightweight breadcrumb for debugging, while still returning None on parse failure.
        if last_error is not None:
            raise last_error

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
    def _fetch_api_pages(
        api_url: str,
        base_params: Dict[str, Any],
        headers: Dict[str, str],
        allowed_hosts: set[str],
    ) -> pd.DataFrame:
        safe_api_url = sanitize_public_http_url(
            api_url,
            allowed_hosts=allowed_hosts,
            allowed_host_suffixes=DATA_GOV_ALLOWED_HOST_SUFFIXES,
        )
        if not safe_api_url:
            raise ValueError("Unsafe API URL.")
        limit = int(base_params.get("limit", 5000))
        offset = 0
        pages: list[pd.DataFrame] = []
        visited = 0
        while len(pages) < 200:
            query = dict(base_params)
            query["offset"] = offset
            response = requests.get(safe_api_url, params=query, timeout=60, headers=headers)
            if not sanitize_public_http_url(
                response.url or safe_api_url,
                allowed_hosts=allowed_hosts,
                allowed_host_suffixes=DATA_GOV_ALLOWED_HOST_SUFFIXES,
            ):
                raise ValueError("Unsafe API redirect URL.")
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
    def _coerce_mixed_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy(deep=True)
        for col in out.columns:
            series = out[col]
            if not (pd.api.types.is_object_dtype(series.dtype) or pd.api.types.is_string_dtype(series.dtype)):
                continue
            if series.dropna().empty:
                continue

            candidate = series.astype(str).str.strip().str.replace(",", "", regex=False)
            candidate_numeric = pd.to_numeric(candidate, errors="coerce")
            numeric_ratio = candidate_numeric.notna().mean()
            if numeric_ratio >= 0.8:
                out[col] = candidate_numeric
        return out

    @staticmethod
    def _reconcile_tamil_progress(df: pd.DataFrame, source: Dict[str, Any], raw_root: Path,
                                  raw_paths: list[Path]) -> tuple[pd.DataFrame, Path | None, dict | None]:
        """Use pinned parliamentary cells for a known corrupt OGD CSV export.

        The CSV dropped decimal points, including values that still pass a
        0-100 range check. Never infer a blanket divisor or apply this repair to
        another input snapshot; both documents and row identities are checked.
        """
        source_id = "data_gov_in_nhai_tamil_nh_major_ongoing_2024_2026"
        if source.get("source_id") != source_id:
            return df, None, None
        csv_sha = "2b2e98b196e10d006292707a584fa49f0fdc82035d6a0f4c00f3cf2d44d9f65c"
        pdf_sha = "46a17f9f1efc8d0afda41cfc5ea41b6f7dd69696aa2b2db6d9ae558c7d739267"
        url = "https://sansad.in/getFile/annex/265/AU286_APrEba.pdf?source=pqars"
        if source.get("progress_reference_url") != url or source.get("progress_reference_sha256") != pdf_sha:
            raise ValueError("Tamil progress requires the approved checksum-pinned Annexure II reference")
        if not any(path.suffix == ".csv" and path.exists() and sha256_for_file(path) == csv_sha for path in raw_paths):
            raise ValueError("Tamil CSV snapshot changed; progress reconciliation requires a new evidence review")
        required = {"sl._no.", "project_name", "implementing_agency", "appointed_date", "length_km", "tpc_rs_in_crore", "physical_progress_pct"}
        if not required <= set(df) or len(df) != 55 or list(pd.to_numeric(df["sl._no."], errors="coerce")) != list(range(1, 56)):
            raise ValueError("Tamil progress project row contract changed")
        from pipelines.connectors.primary_disclosures import download_document
        import pdfplumber
        import math
        import re
        document = raw_root / source_id / "progress_reference_AU286_20240724.pdf"
        if not document.exists():
            download_document(url, document, max_bytes=8_000_000)
        if sha256_for_file(document) != pdf_sha:
            raise ValueError("Tamil parliamentary reference changed; re-extraction required")
        rows = []
        with pdfplumber.open(document) as pdf:
            for page_number, page in enumerate(pdf.pages[2:], start=3):
                for table in page.extract_tables():
                    for row in table:
                        if len(row) == 7 and row[0] and re.fullmatch(r"\d+", row[0].strip()):
                            rows.append((page_number, row))
        if len(rows) != 55:
            raise ValueError("Tamil parliamentary reference does not contain all 55 project rows")
        values, anchors = [], []
        compact = lambda value: re.sub(r"[^a-z0-9]", "", str(value).lower())
        for index, (page_number, row) in enumerate(rows):
            source_row = df.iloc[index]
            nhai = index < 34
            expected_serial = index + 1 if nhai else index - 33
            expected_agency = "National Highways Authority of India (NHAI)" if nhai else "State Public Works Department (PWD)"
            if int(row[0]) != expected_serial or source_row["implementing_agency"] != expected_agency:
                raise ValueError("Tamil parliamentary project agency/order mismatch")
            if compact(source_row["appointed_date"]) != compact(row[4]):
                raise ValueError("Tamil parliamentary project appointed-date mismatch")
            for column, cell in (("length_km", row[2]), ("tpc_rs_in_crore", row[3])):
                expected = pd.to_numeric(cell.replace(",", ""), errors="coerce")
                actual = pd.to_numeric(source_row[column], errors="coerce")
                if not ((pd.isna(actual) and pd.isna(expected)) or (pd.notna(actual) and pd.notna(expected) and math.isclose(float(actual), float(expected), rel_tol=1e-8))):
                    raise ValueError(f"Tamil parliamentary project {column} mismatch")
            value = pd.to_numeric(row[6].strip().rstrip("%"), errors="coerce")
            if pd.notna(value) and not 0 <= value <= 100:
                raise ValueError("Tamil parliamentary progress itself is outside 0-100")
            values.append(value)
            anchors.append(f"PDF p{page_number}; Annexure II; {'NHAI' if nhai else 'State PWD'} row {expected_serial}")
        out = df.copy(deep=True)
        out["physical_progress_raw_csv"] = out["physical_progress_pct"]
        out["physical_progress_pct"] = values
        out["progress_reference_url"] = url
        out["progress_source_document_sha256"] = pdf_sha
        out["progress_citation_anchor"] = anchors
        out["source_as_of_date"] = "2024-07-24"
        changed = int((out["physical_progress_pct"].fillna(-1) != out["physical_progress_raw_csv"].fillna(-1)).sum())
        return out, document, {"reference_url": url, "reference_sha256": pdf_sha, "raw_csv_sha256": csv_sha,
                               "rows_checked": len(rows), "progress_cells_corrected": changed,
                               "transformation": "Exact Annexure II physical-progress cells; original CSV values retained; no inferred scaling"}

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

    @staticmethod
    def _resolve_api_key(source: Dict[str, Any]) -> str:
        api_key_name = source.get("api_key_env")
        api_key = ""
        if isinstance(api_key_name, str) and api_key_name:
            api_key = os.getenv(api_key_name, "").strip()

        if not api_key:
            api_key = os.getenv("DATA_GOV_IN_API_KEY", "").strip()

        if not api_key:
            api_key = os.getenv("DATAGOVIN_API_KEY", "").strip()

        return api_key

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
        allowed_hosts = collect_allowed_hosts_from_source(source)

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
        api_key = self._resolve_api_key(source)
        if api_url and resource_id and api_key and "{" in api_url:
            api_url = api_url.format(resource_id=resource_id)

        unresolved_template = bool(api_url and "{" in api_url and "}" in api_url)
        can_use_api = bool(api_url and resource_id and api_key and not unresolved_template)

        if api_url and not can_use_api and not skip_reason:
            if unresolved_template:
                skip_reason = "api_skipped_unresolved_url_template"
            elif not resource_id:
                skip_reason = "api_skipped_missing_resource_id"
            elif not api_key:
                skip_reason = "api_skipped_missing_api_key"

        if can_use_api:
            params = {
                "api-key": api_key,
                "format": "json",
                "limit": 5000,
            }
            raw_path: Path | None = None
            try:
                api_df = self._fetch_api_pages(api_url, params, self._api_headers(source), allowed_hosts)
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
            explicit_files = self._safe_url_list(source.get("resource_file_urls"))
            page_status_issue = None
            try:
                page_html = ""
                if explicit_files:
                    candidates = self._collect_resource_file_urls(source, page_html)
                else:
                    safe_resource_url = sanitize_public_http_url(
                        resource_url or "",
                        allowed_hosts=allowed_hosts,
                        allowed_host_suffixes=DATA_GOV_ALLOWED_HOST_SUFFIXES,
                    ) if resource_url else None
                    page_resp = requests.get(
                        safe_resource_url,
                        timeout=(8, 30),
                        headers={"user-agent": "BHAI-research-scan/0.2"},
                    ) if safe_resource_url else None
                    if page_resp is not None:
                        if not sanitize_public_http_url(
                            page_resp.url or safe_resource_url or "",
                            allowed_hosts=allowed_hosts,
                            allowed_host_suffixes=DATA_GOV_ALLOWED_HOST_SUFFIXES,
                        ):
                            page_status_issue = "resource_page_unsafe_redirect"
                            page_resp = None
                            page_html = ""
                        else:
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
                        parsed_df, parsed_path = self._read_file_candidate(candidate, raw_root, source_id, allowed_hosts)
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
            empty_df = pd.DataFrame()
            write_parquet(empty_df, output_path)
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
                        "title": source.get("dataset_title"),
                        "retrieved_at": now,
                        "license_terms": source.get("license_terms"),
                        "official_flag": source.get("official_flag", True),
                    },
                    "citations": {
                        "permanent_identifier": source.get("permanent_identifier_hint") or source.get("resource_id")
                        or source.get("resource_id_env"),
                        "anchor": "manual_or_resource_unavailable",
                        "note": "No parsed rows were retrieved from API/page/manual fallback for this source.",
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
                        "row_count": 0,
                        "columns": [],
                    },
                    "retrieved_at": now,
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
        df = self._coerce_mixed_numeric_columns(df)
        df, progress_document, progress_reconciliation = self._reconcile_tamil_progress(df, source, raw_root, raw_paths)
        if progress_document is not None:
            raw_paths.append(progress_document)

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

        if progress_reconciliation:
            manifest["progress_reconciliation"] = progress_reconciliation
            manifest["citations"]["note"] += " Physical progress reconciled against checksum-pinned parliamentary Annexure II; row-level PDF anchors and original CSV values retained."
        if skip_reason:
            manifest["skip_reason"] = skip_reason

        manifest.update(evaluate(df, source | source_meta))
        write_json(manifest, manifest_path)
        return ConnectorResult(source_id=source_id, output_table_path=output_path, manifest=manifest)
