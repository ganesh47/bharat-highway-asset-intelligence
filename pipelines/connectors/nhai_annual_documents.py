from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import parse_qs, urlencode, urljoin, urlsplit, urlunsplit

import pandas as pd
import requests
from pypdf import PdfReader

from .base import ConnectorResult, ConnectorSpec
from pipelines.common import ensure_dirs, sha256_for_file, write_json, write_parquet
from pipelines.quality import evaluate
from pipelines.url_safety import is_public_http_url, sanitize_public_http_url


PDF_EXTENSION_EXTENTIONS = {".pdf", ".PDF"}
ALLOWED_HOST_SUFFIX = "nhai.gov.in"
TRACKING_QUERY_KEYS = {"fbclid", "gclid", "mc_cid", "mc_eid", "ref", "source"}
PDF_MAGIC = b"%PDF-"
ANNUAL_REPORT_TERMS = (
    "annual report",
    "annual-report",
    "annual_report",
    "annualreport",
    "annual reports",
)
ANNUAL_NEGATIVE_TERMS = (
    "press release",
    "press-release",
    "notice",
    "circular",
    "tender",
    "rfp",
    "corrigendum",
)
DATE_PATTERNS = (
    ("%Y-%m-%d", r"\b(20\d{2}-\d{1,2}-\d{1,2})\b"),
    ("%d-%m-%Y", r"\b((?:0[1-9]|[12]\d|3[01])[-/](?:0[1-9]|1[0-2])[-/]20\d{2})\b"),
    ("%Y-%m", r"\b((?:19|20)\d{2}[-/](?:0[1-9]|1[0-2]))\b"),
    ("%Y", r"\b((?:19|20)\d{2})\b"),
)


class NHAIAnnualDocumentsConnector:
    spec = ConnectorSpec(
        name="nhai_annual_documents",
        version="0.1.1",
        source_ids=["nhai_audited_results_pdf", "nhai_annual_report_documents"],
        inputs=["source_inventory.source_item"],
        outputs=["parquet"],
        citation_mapping={
            "primary_source": "publisher_org+dataset_title",
            "permanent_identifier": "permanent_identifier_hint",
            "license_terms": "license_terms",
            "anchor": "document_url",
        },
    )

    PDF_LINK_RE = re.compile(r"https?://[^\"'\s]+\.pdf(?:\?[^\s'\"]*)?", flags=re.IGNORECASE)
    FINANCIAL_YEAR_RE = re.compile(r"\b(20\d{2})[-/](\d{2})\b")
    YEAR_RE = re.compile(r"\b((?:19|20)\d{2})\b")
    AUDITED_FN_RE = re.compile(r"Audited[_\\-\\s]*Results[_\\-\\s]*(\\d{4}[-/]\\d{2})", flags=re.IGNORECASE)

    def _safe_url_list(self, value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        if isinstance(value, (list, tuple, set)):
            return [str(item).strip() for item in value if str(item).strip()]
        return []

    def _safe_text(self, value: Any) -> str:
        if value is None:
            return ""
        text = str(value).strip()
        return text.replace("\xa0", " ")

    @staticmethod
    def _normalize_url(url: str) -> str:
        if not url:
            return ""
        value = url.strip()
        if value.startswith("//"):
            return "https:" + value
        split = urlsplit(value)
        if not split.scheme or not split.netloc:
            return value
        query_items = []
        for key, values in sorted(parse_qs(split.query, keep_blank_values=True).items()):
            key_l = key.lower()
            if key_l.startswith("utm_") or key_l in TRACKING_QUERY_KEYS:
                continue
            for item in values:
                query_items.append((key, item))
        path = re.sub(r"/{2,}", "/", split.path or "/").rstrip("/")
        path = path or "/"
        return urlunsplit(
            (
                split.scheme.lower(),
                split.netloc.lower(),
                path,
                urlencode(query_items, doseq=True),
                "",
            )
        )

    @staticmethod
    def _is_allowed_document_url(url: str) -> bool:
        return is_public_http_url(url, allowed_host_suffixes={ALLOWED_HOST_SUFFIX})

    @staticmethod
    def _looks_like_pdf_payload(content_type: str, sample: bytes, url: str) -> bool:
        ctype = (content_type or "").lower()
        if sample.startswith(PDF_MAGIC):
            return True
        if "application/pdf" not in ctype:
            return False
        if sample.lstrip().startswith((b"<html", b"<!doctype html", b"<?xml")):
            return False
        return url.lower().endswith(".pdf")

    def _annual_report_score(self, title: str, document_url: str, pdf_text: str | None = None, source_hint: Any = None) -> float:
        blob_parts = [self._safe_text(title).lower(), self._normalize_url(document_url).lower(), self._safe_text(source_hint).lower()]
        if pdf_text:
            blob_parts.append(self._safe_text(pdf_text)[:2000].lower())
        blob = " ".join(part for part in blob_parts if part)

        score = 0.0
        if any(term in blob for term in ANNUAL_REPORT_TERMS):
            score += 0.45
        elif "annual" in blob and "report" in blob:
            score += 0.3

        if "annual-reports" in blob or "annual_report" in blob or "annual-report" in blob:
            score += 0.2

        if self.FINANCIAL_YEAR_RE.search(blob) or re.search(r"\b(19|20)\d{2}\s*-\s*(?:19|20)?\d{2}\b", blob):
            score += 0.2
        elif self.YEAR_RE.search(blob):
            score += 0.1

        if pdf_text and any(marker in blob for marker in ("annual report of", "chairman", "management discussion", "performance overview")):
            score += 0.15

        if any(term in blob for term in ANNUAL_NEGATIVE_TERMS):
            score -= 0.35
        if "green cover index" in blob:
            score -= 0.25

        return max(0.0, min(score, 1.0))

    def _dedupe_paths(self, paths: list[Path]) -> list[Path]:
        unique: list[Path] = []
        seen: set[tuple[str, str]] = set()
        for path in paths:
            if not path or not path.exists():
                continue
            key = (str(path), sha256_for_file(path))
            if key in seen:
                continue
            seen.add(key)
            unique.append(path)
        return unique

    def _manual_files(self, source_id: str, raw_root: Path) -> List[Path]:
        manual_dir = raw_root / "manual"
        files: List[Path] = []
        for candidate in (
            manual_dir / f"{source_id}.csv",
            manual_dir / f"{source_id}.json",
            manual_dir / f"{source_id}.parquet",
            manual_dir / f"{source_id}.xlsx",
            manual_dir / f"{source_id}.pdf",
        ):
            if candidate.exists():
                files.append(candidate)

        for suffix in ("csv", "json", "parquet", "xlsx", "pdf"):
            pattern = f"{source_id}_*.{suffix}"
            for candidate in sorted(manual_dir.glob(pattern)):
                if candidate.is_file() and candidate not in files:
                    files.append(candidate)
        return files

    @staticmethod
    def _write_raw_response(raw_root: Path, source_id: str, payload: str | bytes, extension: str) -> Path:
        raw_root.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        out = raw_root / source_id / f"raw_{ts}{extension}"
        out.parent.mkdir(parents=True, exist_ok=True)
        mode = "wb" if isinstance(payload, (bytes, bytearray)) else "w"
        kwargs = {"encoding": "utf-8"} if mode == "w" else {}
        with out.open(mode, **kwargs) as handle:
            handle.write(payload)
        return out

    @staticmethod
    def _parse_date_hint(value: Any) -> str | None:
        if not value:
            return None
        text = str(value).strip()
        if not text:
            return None
        for fmt, pattern in DATE_PATTERNS:
            match = re.search(pattern, text)
            if not match:
                continue
            token = match.group(1)
            try:
                parsed = datetime.strptime(token, fmt)
                if fmt == "%Y":
                    return f"{parsed.year}-01-01"
                if fmt == "%Y-%m":
                    return f"{parsed.year:04d}-{parsed.month:02d}-01"
                return parsed.date().isoformat()
            except Exception:
                continue
        return None

    @staticmethod
    def _guess_financial_year(value: Any) -> str | None:
        if not value:
            return None
        text = str(value).replace("–", "-").replace("—", "-")
        match = NHAIAnnualDocumentsConnector.FINANCIAL_YEAR_RE.search(text)
        if match:
            return f"{match.group(1)}-{match.group(2)}"
        match = NHAIAnnualDocumentsConnector.YEAR_RE.search(text)
        if match:
            return match.group(1)
        return None

    def _collect_urls(self, value: Any, *, base_url: str | None = None) -> set[str]:
        found: set[str] = set()
        if value is None:
            return found

        if isinstance(value, str):
            for match in self.PDF_LINK_RE.findall(value):
                found.add(self._normalize_url(match))
            return found

        if isinstance(value, (list, tuple, set)):
            for item in value:
                found.update(self._collect_urls(item, base_url=base_url))
            return found

        if isinstance(value, dict):
            for key, item in value.items():
                if isinstance(item, str) and key in {
                    "file",
                    "fileUrl",
                    "file_url",
                    "url",
                    "source_url",
                    "path",
                    "document",
                    "document_url",
                    "link",
                    "href",
                }:
                    candidate = item.strip()
                    if candidate.startswith("/") and base_url:
                        candidate = urljoin(base_url, candidate)
                    found.update(self._collect_urls(candidate, base_url=base_url))
                else:
                    found.update(self._collect_urls(item, base_url=base_url))
            return found

        return found

    def _read_manual_dataset(self, source_id: str, source: Dict[str, Any], path: Path, now: str) -> pd.DataFrame:
        ext = path.suffix.lower()
        if ext == ".csv":
            df = pd.read_csv(path)
        elif ext == ".json":
            df = pd.read_json(path)
        elif ext in {".xlsx", ".xls"}:
            df = pd.read_excel(path)
        elif ext == ".parquet":
            df = pd.read_parquet(path)
        elif ext == ".pdf":
            return pd.DataFrame(
                [
                    {
                        "metric_name": "nhai_annual_document_manual",
                        "metric_value": 1.0,
                        "unit": "binary",
                        "document_title": path.name,
                        "source_document_url": str(path.as_posix()),
                        "financial_year": self._guess_financial_year(path.name),
                    }
                ]
            )
        else:
            return pd.DataFrame()

        return self._coerce_df_for_source(source_id, source, df, now)

    @staticmethod
    def _coerce_df_for_source(source_id: str, source: Dict[str, Any], df: pd.DataFrame, now: str) -> pd.DataFrame:
        if df.empty:
            return df
        if "source_id" not in df.columns:
            df["source_id"] = source_id
        if "source_type" not in df.columns:
            df["source_type"] = "official_measured"
        if "metric_category" not in df.columns:
            df["metric_category"] = "official_measured"
        if "dataset_source" not in df.columns:
            df["dataset_source"] = source.get("dataset_title")
        if "retrieved_at" not in df.columns:
            df["retrieved_at"] = now
        if "metric_name" not in df.columns:
            df["metric_name"] = "nhai_annual_document_record"
        if "metric_value" not in df.columns:
            df["metric_value"] = 1.0
        if "unit" not in df.columns:
            df["unit"] = "binary"
        if "as_of_timestamp" in df.columns:
            # preserve any document-level timestamp from upstream sources where available
            pass
        return df

    def _discover_candidates_from_api(self, endpoint: str, source: Dict[str, Any], terms: list[str]) -> list[Dict[str, Any]]:
        candidates: list[Dict[str, Any]] = []
        pages = int(source.get("discovery_pages", 4))
        page_size = int(source.get("discovery_page_size", 200))

        split = urlsplit(endpoint)
        query_params = {key: value[0] for key, value in parse_qs(split.query).items()}
        if split.scheme and split.netloc:
            endpoint = urlunsplit((split.scheme, split.netloc, split.path, "", ""))
        endpoint = sanitize_public_http_url(endpoint, allowed_host_suffixes={ALLOWED_HOST_SUFFIX}) or ""
        if not endpoint:
            return candidates

        endpoint_l = endpoint.lower()
        if "press" in endpoint_l:
            payload_mode = "press"
        elif "commontype" in endpoint_l:
            payload_mode = "commontype"
        else:
            payload_mode = "policy"

        for page in range(pages):
            if payload_mode == "press":
                payload = {
                    "language": "en",
                    "index": page,
                    "press_release_year": "",
                    "itemsPerPage": page_size,
                    "totalrecord": page_size,
                }
            elif payload_mode == "commontype":
                payload = {
                    "language": "en",
                    "index": page,
                    "itemsPerPage": page_size,
                    "totalrecord": page_size,
                }
            else:
                payload = {"language": "en", "page": page, "limit": page_size}

            payload.update(query_params)

            try:
                response = requests.post(
                    endpoint,
                    data=payload,
                    timeout=45,
                    headers={"user-agent": "BHAI-research-scan/0.3"},
                )
                if not sanitize_public_http_url(response.url or endpoint, allowed_host_suffixes={ALLOWED_HOST_SUFFIX}):
                    break
            except Exception:
                break

            if response.status_code >= 400:
                break

            try:
                payload_json = response.json()
            except Exception:
                break

            records: list[Any] = []
            for key in ("data", "records", "rows", "result", "list", "detail"):
                value = payload_json.get(key) if isinstance(payload_json, dict) else None
                if isinstance(value, list):
                    records = value
                    break
            if not records:
                break

            for item in records:
                title = self._safe_text(item.get("title", ""))
                blob = f"{title} {str(item).lower()}"
                if not any(term.lower() in blob.lower() for term in terms):
                    continue
                for doc_url in self._collect_urls(item, base_url=endpoint):
                    normalized_url = self._normalize_url(doc_url)
                    if ".pdf" not in normalized_url.lower():
                        continue
                    if not self._is_allowed_document_url(normalized_url):
                        continue
                    if self._annual_report_score(title, normalized_url, source_hint=item) < 0.4:
                        continue
                    if not self._probe_pdf_url(normalized_url):
                        continue
                    candidates.append(
                        {
                            "title": title or source.get("dataset_title", ""),
                            "document_url": normalized_url,
                            "source_hint": {"endpoint": endpoint, "mode": payload_mode},
                        }
                    )

            if len(records) < page_size:
                break

        return candidates

    def _discover_audited_candidates(self, source: Dict[str, Any], now: str) -> list[Dict[str, Any]]:
        candidates: list[Dict[str, Any]] = []
        seen_urls: set[str] = set()

        for hint in self._safe_url_list(source.get("resource_file_urls")):
            url = self._normalize_url(hint)
            if not url.lower().endswith(".pdf") or not self._is_allowed_document_url(url):
                continue
            if url not in seen_urls:
                candidates.append({"title": source.get("dataset_title", ""), "document_url": url, "source_hint": "inventory_hint"})
                seen_urls.add(url)

        base = source.get("annual_document_url_prefix") or "https://nhai.gov.in/nhai/sites/default/files/mix_file/"
        configured_years = source.get("financial_years")
        if isinstance(configured_years, (list, tuple)) and configured_years:
            year_candidates = list(configured_years)
        else:
            start_year = int(source.get("start_year", 2015))
            end_year = int(source.get("end_year", datetime.now().year))
            year_candidates = [f"{year}-{(year + 1) % 100:02d}" for year in range(start_year, end_year + 1)]

        for year_value in year_candidates:
            match = self.FINANCIAL_YEAR_RE.match(f"{year_value}".replace("-", "-"))
            if not match:
                continue
            filename = source.get(
                "annual_filename_template",
                "Audited_Results_{year}(SEBI_Format).pdf",
            )
            filename = filename.format(year=year_value)
            url = self._normalize_url(f"{base.rstrip('/')}/{filename}")
            if url in seen_urls:
                continue
            if self._is_allowed_document_url(url) and self._probe_pdf_url(url):
                candidates.append(
                    {
                        "title": f"{source.get('dataset_title', 'NHAI audited results')} {year_value}",
                        "document_url": url,
                        "source_hint": "financial_year_pattern",
                    }
                )
                seen_urls.add(url)

        return candidates

    def _discover_annual_report_candidates(self, source: Dict[str, Any], now: str) -> list[Dict[str, Any]]:
        seen_urls: set[str] = set()
        discovered: list[Dict[str, Any]] = []

        for hint in self._safe_url_list(source.get("resource_file_urls")):
            url = self._normalize_url(hint)
            if url and url.lower().endswith(".pdf") and self._is_allowed_document_url(url):
                key = url.lower()
                if key not in seen_urls:
                    seen_urls.add(key)
                    if self._annual_report_score(source.get("dataset_title", ""), url, source_hint="inventory_hint") >= 0.4 and self._probe_pdf_url(url):
                        discovered.append({"title": source.get("dataset_title", ""), "document_url": url, "source_hint": "inventory_hint"})

        endpoints = source.get(
            "discovery_endpoints",
            [
                "https://nhai.gov.in/nhai/api/policycirculars",
                "https://nhai.gov.in/nhai/api/press-release",
            ],
        )
        terms = ["annual report", "annual reports", "annual", "finance", "audited", "performance", "annual report 202"]
        for endpoint in endpoints:
            for item in self._discover_candidates_from_api(str(endpoint), source, terms):
                url = self._normalize_url(item.get("document_url", ""))
                if not url.lower().endswith(".pdf") or url in seen_urls:
                    continue
                seen_urls.add(url)
                if self._probe_pdf_url(url):
                    discovered.append(item)

        page = source.get("resource_page_url") or source.get("url")
        if page and self._is_allowed_document_url(page):
            try:
                response = requests.get(page, timeout=25, headers={"user-agent": "BHAI-research-scan/0.3"})
                if not sanitize_public_http_url(response.url or page, allowed_host_suffixes={ALLOWED_HOST_SUFFIX}):
                    return discovered
                if response.ok:
                    for link in self._collect_urls(response.text):
                        normalized_link = self._normalize_url(link)
                        if not normalized_link.lower().endswith(".pdf"):
                            continue
                        if not self._is_allowed_document_url(normalized_link):
                            continue
                        if self._annual_report_score(source.get("dataset_title", ""), normalized_link, source_hint=page) < 0.4:
                            continue
                        key = normalized_link.lower()
                        if key in seen_urls:
                            continue
                        if self._probe_pdf_url(normalized_link):
                            seen_urls.add(key)
                            discovered.append({"title": source.get("dataset_title", ""), "document_url": normalized_link, "source_hint": page})
            except Exception:
                pass

        return discovered

    def _probe_pdf_url(self, url: str) -> bool:
        normalized_url = self._normalize_url(url)
        if not self._is_allowed_document_url(normalized_url):
            return False
        try:
            response = requests.head(normalized_url, timeout=20, headers={"user-agent": "BHAI-research-scan/0.3"}, allow_redirects=True)
            if response.status_code >= 400:
                return False
            final_url = self._normalize_url(response.url or normalized_url)
            if not self._is_allowed_document_url(final_url):
                return False
            head_type = (response.headers.get("Content-Type") or "").lower()
            if head_type and "application/pdf" not in head_type and "text/html" in head_type:
                return False
        except Exception:
            final_url = normalized_url
        try:
            response = requests.get(
                final_url,
                timeout=25,
                stream=True,
                headers={"user-agent": "BHAI-research-scan/0.3", "Range": "bytes=0-2047"},
                allow_redirects=True,
            )
            if response.status_code >= 400:
                return False
            resolved_url = self._normalize_url(response.url or final_url)
            if not self._is_allowed_document_url(resolved_url):
                return False
            sample = b""
            for chunk in response.iter_content(chunk_size=2048):
                if chunk:
                    sample += chunk
                if len(sample) >= 2048:
                    break
            ctype = response.headers.get("Content-Type") or ""
            return self._looks_like_pdf_payload(ctype, sample[:2048], resolved_url)
        except Exception:
            return False

    @staticmethod
    def _pdf_metadata(path: Path) -> tuple[str | None, str | None]:
        try:
            reader = PdfReader(str(path))
            page_text = "\n".join((page.extract_text() or "") for page in reader.pages[:2])
            return NHAIAnnualDocumentsConnector._parse_date_hint(page_text), page_text
        except Exception:
            return None, None

    def _extract_candidate_as_of(self, source: Dict[str, Any], candidate: Dict[str, Any], headers: dict[str, str], pdf_text: str | None) -> str | None:
        hints = [
            candidate.get("as_of"),
            source.get("as_of"),
            source.get("publication_date"),
            source.get("date"),
            headers.get("Last-Modified"),
            headers.get("Date"),
        ]
        for hint in hints:
            parsed = self._parse_date_hint(hint)
            if parsed:
                return parsed
        return self._parse_date_hint(candidate.get("title")) or self._parse_date_hint(pdf_text)

    def _row_from_candidate(
        self,
        source_id: str,
        source: Dict[str, Any],
        candidate: Dict[str, Any],
        raw_root: Path,
        now: str,
    ) -> tuple[dict[str, Any] | None, Path | None]:
        document_url = self._normalize_url(candidate.get("document_url", ""))
        if not document_url or not self._is_allowed_document_url(document_url):
            return None, None

        try:
            response = requests.get(document_url, timeout=60, headers={"user-agent": "BHAI-research-scan/0.3"}, allow_redirects=True)
        except Exception:
            return None, None
        if not response.ok:
            return None, None

        resolved_url = self._normalize_url(response.url or document_url)
        if not self._is_allowed_document_url(resolved_url):
            return None, None

        ctype = response.headers.get("Content-Type") or ""
        if not self._looks_like_pdf_payload(ctype, response.content[:2048], resolved_url):
            return None, None

        raw_pdf = self._write_raw_response(raw_root / source_id, source_id, response.content, ".pdf")
        publication_date, pdf_text = self._pdf_metadata(raw_pdf)
        as_of = self._extract_candidate_as_of(source, candidate, response.headers, pdf_text)
        if not as_of:
            as_of = publication_date

        title = self._safe_text(candidate.get("title", "")) or raw_pdf.name
        doc_year = (
            self._guess_financial_year(title)
            or self._guess_financial_year(resolved_url)
            or self._guess_financial_year(pdf_text)
        )
        if source_id == "nhai_annual_report_documents":
            report_score = self._annual_report_score(title, resolved_url, pdf_text, candidate.get("source_hint"))
            if report_score < 0.4:
                raw_pdf.unlink(missing_ok=True)
                return None, None
        metric_name = (
            "nhai_audited_results_document"
            if source_id == "nhai_audited_results_pdf"
            else "nhai_annual_report_document"
        )
        checksum = hashlib.sha256((response.content if response.content else b"")).hexdigest()

        return (
            {
                "source_id": source_id,
                "source_type": "official_measured",
                "metric_category": "official_measured",
                "dataset_source": source.get("dataset_title"),
                "metric_name": metric_name,
                "metric_value": 1.0,
                "unit": "binary",
                "document_title": title,
                "source_document_url": resolved_url,
                "financial_year": doc_year,
                "as_of_timestamp": as_of,
                "publication_date": publication_date,
                "document_checksum": checksum,
                "retrieved_at": now,
            },
            raw_pdf,
        )

    def _build_manifest(
        self,
        source_id: str,
        source: Dict[str, Any],
        output_path: Path,
        rows: pd.DataFrame,
        raw_files: list[Path],
        now: str,
        status: str,
        status_note: str,
        anchor: str,
    ) -> dict[str, Any]:
        manifest = {
            "source_id": source_id,
            "connector": self.spec.name,
            "version": self.spec.version,
            "status": status,
            "metric_category": "official_measured",
            "source": {
                "publisher": source.get("publisher_org"),
                "title": source.get("dataset_title"),
                "url": source.get("url") or source.get("resource_page_url"),
                "retrieved_at": now,
                "license_terms": source.get("license_terms"),
                "official_flag": source.get("official_flag", True),
                "domain": source.get("domain"),
            },
            "citations": {
                "permanent_identifier": source.get("permanent_identifier_hint") or source_id,
                "anchor": anchor,
                "note": status_note,
            },
            "manifest": {
                "raw_files": [
                    {"path": str(path), "sha256": sha256_for_file(path), "size_bytes": path.stat().st_size}
                    for path in self._dedupe_paths(raw_files)
                    if path and path.exists()
                ],
                "output_files": [
                    {"path": str(output_path), "format": "parquet", "sha256": sha256_for_file(output_path)}
                ],
                "row_count": int(len(rows)),
                "columns": list(rows.columns),
            },
            "retrieved_at": now,
        }
        return manifest

    def run(self, source: Dict[str, Any], raw_root: Path, processed_root: Path, manifest_root: Path) -> ConnectorResult:
        source_id = source["source_id"]
        output_path = processed_root / f"{source_id}.parquet"
        manifest_path = manifest_root / f"{source_id}.json"
        ensure_dirs(raw_root.as_posix(), processed_root.as_posix(), manifest_root.as_posix())
        now = datetime.now(timezone.utc).isoformat()

        manual_rows = []
        raw_files: list[Path] = []
        for path in self._manual_files(source_id, raw_root):
            df = self._read_manual_dataset(source_id, source, path, now)
            if df.empty:
                continue
            df = self._coerce_df_for_source(source_id, source, df, now)
            manual_rows.append(df)
            raw_files.append(path)

        if manual_rows:
            rows = pd.concat(manual_rows, ignore_index=True)
            rows = rows.drop_duplicates(subset=["source_document_url", "metric_name", "financial_year"], keep="first").reset_index(drop=True)
            write_parquet(rows, output_path)

            manifest = self._build_manifest(
                source_id=source_id,
                source=source,
                output_path=output_path,
                rows=rows,
                raw_files=raw_files,
                now=now,
                status="manual_ingest",
                status_note="Manual source artifacts ingested for this source.",
                anchor="manual_document_drop",
            )
            manifest["metric_category"] = "official_measured"
            manifest.update(evaluate(rows, source | manifest["source"]))
            write_json(manifest, manifest_path)
            return ConnectorResult(source_id=source_id, output_table_path=output_path, manifest=manifest)

        if source_id == "nhai_audited_results_pdf":
            candidate_rows = self._discover_audited_candidates(source, now)
        else:
            candidate_rows = self._discover_annual_report_candidates(source, now)

        rows_payload: list[dict[str, Any]] = []
        seen_documents: set[tuple[str, str]] = set()
        for candidate in candidate_rows:
            row, raw_file = self._row_from_candidate(source_id, source, candidate, raw_root, now)
            if row is None:
                continue
            dedupe_key = (
                str(row.get("document_checksum") or ""),
                str(row.get("source_document_url") or ""),
            )
            if dedupe_key in seen_documents:
                continue
            seen_documents.add(dedupe_key)
            rows_payload.append(row)
            if raw_file is not None:
                raw_files.append(raw_file)

        if not rows_payload:
            rows = pd.DataFrame(
                [
                    {
                        "source_id": source_id,
                        "source_type": "official_measured",
                        "metric_category": "official_measured",
                        "dataset_source": source.get("dataset_title"),
                        "metric_name": "nhai_annual_document_discovery_status",
                        "metric_value": 0.0,
                        "unit": "binary",
                        "status_note": "No downloadable official PDFs discovered.",
                        "retrieved_at": now,
                    }
                ]
            )
            write_parquet(rows, output_path)
            manifest = self._build_manifest(
                source_id=source_id,
                source=source,
                output_path=output_path,
                rows=rows,
                raw_files=raw_files,
                now=now,
                status="stubs_disabled",
                status_note="No downloadable official PDFs discovered.",
                anchor="no_discoverable_documents",
            )
            manifest["skip_reason"] = "no_discoverable_documents"
            manifest["metric_category"] = "official_measured"
            manifest.update(evaluate(rows, source | manifest["source"]))
            write_json(manifest, manifest_path)
            return ConnectorResult(
                source_id=source_id,
                output_table_path=output_path,
                manifest=manifest,
                skipped=True,
                skip_reason="no_discoverable_documents",
            )

        rows = pd.DataFrame(rows_payload)
        rows = rows.drop_duplicates(subset=["source_document_url", "metric_name", "financial_year"], keep="first").reset_index(drop=True)
        rows = self._coerce_df_for_source(source_id, source, rows, now)
        write_parquet(rows, output_path)

        first_url = (rows_payload[0].get("source_document_url") if rows_payload else "discovery")
        manifest = self._build_manifest(
            source_id=source_id,
            source=source,
            output_path=output_path,
            rows=rows,
            raw_files=raw_files,
            now=now,
            status="automated",
            status_note="Official NHAI document discovery and ingestion succeeded.",
            anchor=f"discovery:{first_url}",
        )
        manifest["metric_category"] = "official_measured"
        manifest.update(evaluate(rows, source | manifest["source"]))
        write_json(manifest, manifest_path)
        return ConnectorResult(source_id=source_id, output_table_path=output_path, manifest=manifest)
