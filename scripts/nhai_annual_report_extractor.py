from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd
import requests
from pypdf import PdfReader


try:  # optional
    import camelot  # type: ignore

    HAS_CAMELOT = True
except Exception:
    camelot = None
    HAS_CAMELOT = False

try:
    import pdfplumber  # type: ignore

    HAS_PDFPLUMBER = True
except Exception:
    pdfplumber = None
    HAS_PDFPLUMBER = False

try:
    import tabula  # type: ignore

    HAS_TABULA = True
except Exception:
    tabula = None
    HAS_TABULA = False

NUMERIC_RE = re.compile(r"-?\d{1,3}(?:,\d{3})*(?:\.\d+)?")
TUPLE_LINE_RE = re.compile(r"^\s*(\d+[\.]?)\s+(.+?)\s{2,}(\d+[\d,]*\.?\d*)\s*$")
KV_PATTERN = re.compile(
    r"(?P<label>[\w/().,%&\-+\s]{3,220})[:\-–—]\s*(?P<value>-?(?:Rs\.?|₹|INR)?\s*[\d,]+(?:\.\d+)?(?:\s*(?:crore|crores|lakh|lakhs|million|billion|bn|%|nos?|No\.?|N\.A\.?))?)",
    flags=re.IGNORECASE,
)
MULTI_SPACE_RE = re.compile(r"\s{2,}")
NOISE_LINE_RE = re.compile(
    r"^(?:page\s+\d+|\d+|https?://\S+|www\.\S+|national highways authority of india|annual report(?: of nhai)?|contents?)$",
    flags=re.IGNORECASE,
)
MAX_PDF_BYTES = 150 * 1024 * 1024
MAX_PDF_PAGES = 250
DOWNLOAD_CHUNK_BYTES = 1024 * 1024

CANONICAL_COLUMNS = [
    "report_year",
    "report_year_start",
    "source_document_url",
    "source_document_title",
    "source_document_sha256",
    "source_id",
    "source_type",
    "dataset_source",
    "dataset_created_at",
    "lineage_dataset_created_at",
    "lineage_output_file",
    "lineage_document_url",
    "lineage_document_title",
    "lineage_source_id",
    "lineage_source_type",
    "lineage_report_year",
    "lineage_pdf_checksum",
    "page_no",
    "table_id",
    "row_no",
    "col_no",
    "row_index",
    "record_type",
    "metric_category",
    "metric_name",
    "metric_value_numeric",
    "metric_value_text",
    "metric_unit",
    "extraction_method",
    "parser_name",
    "parser_metadata",
    "extraction_confidence",
    "ocr_attempted",
    "quality_flag",
    "raw_line",
    "pdf_checksum",
    "citations",
]


def _safe_float(raw: str) -> Tuple[float | None, str | None]:
    token = raw.strip()
    unit = None
    tlow = token.lower()
    if not token:
        return None, None

    for candidate in ["crore", "crores", "lakh", "lakhs", "million", "billion", "bn", "%", "nos", "no.", "n.a.", "count"]:
        if re.search(rf"\\b{re.escape(candidate)}\\b", tlow):
            if candidate in {"%"}:
                unit = "%"
            elif candidate in {"crore", "crores"}:
                unit = "crore"
            elif candidate in {"lakh", "lakhs"}:
                unit = "lakh"
            elif candidate in {"million"}:
                unit = "million"
            elif candidate in {"billion", "bn"}:
                unit = "billion"
            elif candidate in {"nos", "no.", "count", "n.a."}:
                unit = "count"
            break

    cleaned = token.replace(",", "")
    cleaned = re.sub(r"[()₹Rs\sINR%]", "", cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.strip()
    if cleaned in {"", "-", "--", "+"}:
        return None, unit

    try:
        return float(cleaned), unit
    except Exception:
        return None, unit


def _normalize_year(value: Any) -> str:
    if value is None:
        return "unknown"
    text = str(value).strip()
    if not text:
        return "unknown"

    m = re.search(r"(20\d{2})-(\d{2})", text)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    m = re.search(r"(19\d{2})-(\d{2})", text)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    m = re.search(r"(20\d{2}|19\d{2})", text)
    if m:
        return m.group(1)
    return text


def _year_start(year_label: str) -> int:
    if not year_label or year_label == "unknown":
        return 0
    m = re.match(r"(20\d{2}|19\d{2})-(\d{2})", year_label)
    if m:
        return int(m.group(1))
    m = re.match(r"(20\d{2}|19\d{2})", year_label)
    if m:
        return int(m.group(1))
    return 0


def _checksum(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def _safe_path(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _parser_environment() -> dict[str, bool]:
    return {
        "camelot": HAS_CAMELOT,
        "pdfplumber": HAS_PDFPLUMBER,
        "tabula": HAS_TABULA,
        "tesseract": shutil.which("tesseract") is not None,
        "pdftotext": shutil.which("pdftotext") is not None,
        "pdftoppm": shutil.which("pdftoppm") is not None,
    }


def _json_object(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            value = json.loads(raw)
            if isinstance(value, dict):
                return value
        except Exception:
            return {"raw": raw}
    return {}


def _json_text(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _is_noise_line(line: str) -> bool:
    text = MULTI_SPACE_RE.sub(" ", (line or "").strip())
    if not text:
        return True
    if len(text) < 4:
        return True
    if NOISE_LINE_RE.match(text):
        return True
    if not any(ch.isalnum() for ch in text):
        return True
    if text.count("|") >= 6 and not any(ch.isalpha() for ch in text):
        return True
    return False


def _canonicalize_row(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    aliases = {
        "page_number": "page_no",
        "unit": "metric_unit",
        "metric_value": "metric_value_text",
        "document_title": "source_document_title",
        "document_url": "source_document_url",
    }
    for src, dst in aliases.items():
        if src in out and dst not in out:
            out[dst] = out.pop(src)
    return out


def _quality_flag_for(confidence: float, extraction_method: str) -> str:
    if extraction_method in {"error", "failed"} or confidence <= 0:
        return "failed"
    if confidence >= 0.80:
        return "high"
    if confidence >= 0.45:
        return "medium"
    return "low"


def _score_row_confidence(
    row: dict[str, Any],
    parser_env: dict[str, bool],
    parsers_attempted: list[str],
) -> tuple[float, list[str]]:
    method = str(row.get("extraction_method", "") or "failed").lower()
    parser_name = str(row.get("parser_name", "") or "none").lower()
    base_scores = {
        "table": 0.86,
        "text": 0.60,
        "ocr": 0.48,
        "error": 0.0,
        "failed": 0.0,
    }
    confidence = base_scores.get(method, 0.35)
    notes: list[str] = []

    if parser_name == "camelot":
        confidence += 0.06
    elif parser_name in {"pdfplumber", "tabula"}:
        confidence += 0.03
    elif parser_name == "ocr":
        notes.append("ocr_fallback")

    raw_line = str(row.get("raw_line", "") or row.get("metric_value_text", "") or "")
    if _is_noise_line(raw_line):
        confidence -= 0.18
        notes.append("noise_suspected")

    if row.get("metric_value_numeric") is not None:
        confidence += 0.06
    else:
        notes.append("numeric_missing")

    if row.get("source_document_url"):
        confidence += 0.03
    if row.get("source_document_sha256") or row.get("pdf_checksum"):
        confidence += 0.03

    if method == "text" and not parser_env.get("pdftotext", False):
        confidence -= 0.08
        notes.append("text_tool_unavailable")
    if method == "ocr" and not parser_env.get("tesseract", False):
        confidence -= 0.20
        notes.append("ocr_tool_unavailable")
    if not parsers_attempted:
        confidence -= 0.10
        notes.append("missing_parser_trace")

    confidence = max(0.0, min(1.0, confidence))
    return round(confidence, 3), notes


def _apply_parser_trace(
    rows: list[dict[str, Any]],
    parser_env: dict[str, bool],
    parsers_attempted: list[str],
    first_success_parser: str,
    parser_failure_reason: str | None,
) -> list[dict[str, Any]]:
    for idx, row in enumerate(rows):
        row = _canonicalize_row(row)
        meta = _json_object(row.get("parser_metadata"))
        confidence, notes = _score_row_confidence(row, parser_env, parsers_attempted)
        meta.update(
            {
                "parser_environment": parser_env,
                "parsers_attempted": parsers_attempted,
                "attempt_count": len(parsers_attempted),
                "first_success_parser": first_success_parser,
                "parser_failure_reason": parser_failure_reason,
                "row_quality_notes": notes,
            }
        )
        row["parser_name"] = row.get("parser_name") or first_success_parser or "none"
        row["parser_metadata"] = _json_text(meta)
        row["extraction_confidence"] = confidence
        row["quality_flag"] = _quality_flag_for(confidence, str(row.get("extraction_method", "")))
        rows[idx] = row
    return rows


def _dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for raw_row in rows:
        row = _canonicalize_row(raw_row)
        key = (
            row.get("page_no", 0),
            row.get("table_id", ""),
            str(row.get("record_type", "")).lower(),
            MULTI_SPACE_RE.sub(" ", str(row.get("metric_name", "")).strip()).lower(),
            MULTI_SPACE_RE.sub(" ", str(row.get("metric_value_text", row.get("raw_line", ""))).strip()).lower(),
            row.get("metric_value_numeric"),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def _download_pdf(url: str) -> tuple[bytes | None, str | None]:
    try:
        response = requests.get(url, timeout=120, headers={"user-agent": "BHAI-multiagent-extractor/0.2"}, stream=True)
        response.raise_for_status()
    except Exception as exc:
        return None, f"download_failed:{exc}"

    chunks: list[bytes] = []
    total = 0
    try:
        for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
            if not chunk:
                continue
            total += len(chunk)
            if total > MAX_PDF_BYTES:
                return None, f"pdf_too_large:{total}"
            chunks.append(chunk)
    finally:
        response.close()

    payload = b"".join(chunks)
    ctype = str(response.headers.get("content-type", "")).lower()
    if not payload.startswith(b"%PDF") and "pdf" not in ctype:
        return None, f"invalid_pdf_content_type:{ctype or 'missing'}"
    return payload, None


def _extract_text_pdftotext(pdf_path: Path) -> str:
    if shutil.which("pdftotext") is None:
        return ""
    proc = subprocess.run(
        ["pdftotext", "-layout", "-nopgbrk", str(pdf_path), "-"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=300,
        check=False,
    )
    if proc.returncode != 0:
        return ""
    return proc.stdout.decode("utf-8", errors="ignore")


def _extract_text_pdfreader(pdf_path: Path) -> List[str]:
    try:
        reader = PdfReader(str(pdf_path))
    except Exception:
        return []

    return [(page.extract_text() or "") for page in reader.pages]


def _ocr_page(pdf_path: Path, page_no: int) -> str:
    if shutil.which("pdftoppm") is None or shutil.which("tesseract") is None:
        return ""

    with tempfile.TemporaryDirectory(prefix="nhai_ar_ocr_", dir=str(pdf_path.parent)) as d:
        base = Path(d) / f"page_{page_no}"
        render = subprocess.run(
            [
                "pdftoppm",
                "-f",
                str(page_no),
                "-l",
                str(page_no),
                "-r",
                "200",
                "-png",
                str(pdf_path),
                str(base),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=120,
            check=False,
        )
        if render.returncode != 0:
            return ""

        images = sorted(Path(d).glob(f"page_{page_no}-*.png"))
        if not images:
            return ""

        chunks: List[str] = []
        for image in images[:2]:
            ocr = subprocess.run(
                [
                    "tesseract",
                    str(image),
                    "stdout",
                    "--psm",
                    "6",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=180,
                check=False,
            )
            if ocr.returncode == 0:
                txt = ocr.stdout.decode("utf-8", errors="ignore")
                if txt:
                    chunks.append(txt)
        return "\n".join(chunks)


def _extract_with_camelot(pdf_path: Path, page_no: int) -> list[list[str]]:
    if not HAS_CAMELOT:
        return []
    out: list[list[str]] = []
    for flavor in ["lattice", "stream"]:
        try:
            tables = camelot.read_pdf(str(pdf_path), pages=str(page_no), flavor=flavor)
            for table in tables:
                for row in table.df.values.tolist():
                    if any(str(x).strip() for x in row):
                        out.append([str(x).strip() for x in row])
            if out:
                return out
        except Exception:
            continue
    return out


def _extract_with_pdfplumber(pdf_path: Path, page_no: int) -> list[list[str]]:
    if not HAS_PDFPLUMBER:
        return []
    rows: list[list[str]] = []
    try:
        with pdfplumber.open(str(pdf_path)) as pdf:
            if page_no - 1 >= len(pdf.pages):
                return []
            page = pdf.pages[page_no - 1]
            for table in page.extract_tables() or []:
                for row in table:
                    normalized = [str(x).strip() if x is not None else "" for x in row]
                    if any(normalized):
                        rows.append(normalized)
    except Exception:
        return []
    return rows


def _extract_with_tabula(pdf_path: Path, page_no: int) -> list[list[str]]:
    if not HAS_TABULA:
        return []
    try:
        tables = tabula.read_pdf(str(pdf_path), pages=str(page_no), multiple_tables=True)
        out: list[list[str]] = []
        for table in tables or []:
            df = table
            if df is None or df.empty:
                continue
            for row in df.astype(str).values.tolist():
                normalized = [str(x).strip() for x in row]
                if any(normalized):
                    out.append(normalized)
        return out
    except Exception:
        return []


def _page_has_table_signal(page_text: str) -> bool:
    if not page_text:
        return False
    lines = [line.strip() for line in page_text.splitlines() if line.strip()]
    if len(lines) < 3:
        return False

    scored_lines = 0
    for line in lines[:80]:
        numeric_hits = len(NUMERIC_RE.findall(line))
        if numeric_hits >= 2:
            scored_lines += 1
            continue
        if MULTI_SPACE_RE.search(line) and len(line.split()) >= 3:
            scored_lines += 1
            continue
        if "|" in line and len([part for part in line.split("|") if part.strip()]) >= 3:
            scored_lines += 1
    return scored_lines >= 3


def _table_row_to_extracted(
    page_no: int,
    table_id: str,
    row: list[str],
    row_no: int,
    source_meta: dict[str, Any],
    checksum: str,
    parser_name: str,
    extraction_method: str,
    year: str,
    citations: dict[str, Any],
    ocr_attempted: bool,
    used_parser: bool,
) -> list[dict[str, Any]]:
    text = " | ".join([r for r in row if r])
    if not text:
        return []

    value_numeric = None
    value_unit = None
    metric_name = "table_row"
    metric_text = text

    candidate_vals = [NUMERIC_RE.findall(r) for r in row]
    candidates: list[str] = []
    for cell in row:
        m = NUMERIC_RE.search(cell or "")
        if m:
            candidates.append(m.group(0))
    if candidates:
        value_text = candidates[0]
        value_numeric, value_unit = _safe_float(value_text)
        metric_name = MULTI_SPACE_RE.sub(" ", row[0].strip()) if row and row[0].strip() else "table_row"
        metric_text = value_text if len(candidates) == 1 else metric_text
        if value_unit is None:
            value_unit = None

    col_no = 0
    if row:
        for idx, cell in enumerate(row, start=1):
            c = cell.strip() if isinstance(cell, str) else ""
            if c:
                col_no = idx
                break

    confidence = 0.84 if used_parser else 0.7
    return [
        {
            **source_meta,
            "row_no": int(row_no),
            "col_no": int(col_no),
            "page_no": int(page_no),
            "table_id": table_id,
            "record_type": "table_row",
            "metric_category": "official_measured",
            "metric_name": metric_name[:220] if metric_name else "table_row",
            "metric_value_numeric": value_numeric,
            "metric_value_text": metric_text,
            "metric_unit": value_unit,
            "extraction_method": extraction_method,
            "parser_name": parser_name,
            "parser_metadata": json.dumps({"parser": parser_name, "row_cell_count": len(row)}, ensure_ascii=False),
            "extraction_confidence": confidence,
            "ocr_attempted": bool(ocr_attempted),
            "quality_flag": "high" if used_parser else "medium",
            "raw_line": text[:2000],
            "pdf_checksum": checksum,
            "citations": json.dumps(citations, ensure_ascii=False),
            "report_year": year,
            "report_year_start": _year_start(year),
            "lineage_output_file": source_meta.get("lineage_output_file", ""),
        }
    ]


def _line_text_rows(page_no: int, page_text: str, row_no: int, source_meta: dict[str, Any], checksum: str, year: str, citations: dict[str, Any], ocr_attempted: bool) -> tuple[list[dict[str, Any]], int]:
    lines = [ln.strip() for ln in page_text.splitlines() if ln.strip() and not _is_noise_line(ln)]
    out: list[dict[str, Any]] = []

    for line in lines:
        m = TUPLE_LINE_RE.match(line)
        if m:
            row_index = int(row_no)
            row_no += 1
            label = m.group(2).strip()
            value_token = m.group(3).strip()
            val_num, unit = _safe_float(value_token)
            out.append(
                {
                    **source_meta,
                    "row_no": row_index,
                    "col_no": 1,
                    "page_no": page_no,
                    "table_id": f"p{page_no:04d}:kv",
                    "record_type": "kv_line",
                    "metric_category": "official_measured",
                    "metric_name": label[:220],
                    "metric_value_numeric": val_num,
                    "metric_value_text": value_token,
                    "metric_unit": unit,
                    "extraction_method": "text",
                    "parser_name": "heuristic",
                    "parser_metadata": json.dumps({"line_type": "numbered_toc"}, ensure_ascii=False),
                    "extraction_confidence": 0.84,
                    "ocr_attempted": bool(ocr_attempted),
                    "quality_flag": "medium",
                    "raw_line": line,
                    "pdf_checksum": checksum,
                    "citations": json.dumps(citations, ensure_ascii=False),
                    "report_year": year,
                    "report_year_start": _year_start(year),
                    "lineage_output_file": source_meta.get("lineage_output_file", ""),
                }
            )
            continue

        m = KV_PATTERN.search(line)
        if m:
            row_index = int(row_no)
            row_no += 1
            label = m.group("label").strip()
            value = m.group("value").strip()
            val_num, unit = _safe_float(value)
            if not label:
                continue
            out.append(
                {
                    **source_meta,
                    "row_no": row_index,
                    "col_no": 2,
                    "page_no": page_no,
                    "table_id": f"p{page_no:04d}:kv",
                    "record_type": "kv_line",
                    "metric_category": "official_measured",
                    "metric_name": MULTI_SPACE_RE.sub(" ", label)[:220],
                    "metric_value_numeric": val_num,
                    "metric_value_text": value,
                    "metric_unit": unit,
                    "extraction_method": "text",
                    "parser_name": "heuristic",
                    "parser_metadata": json.dumps({"line_type": "kv"}, ensure_ascii=False),
                    "extraction_confidence": 0.78 if val_num is not None else 0.55,
                    "ocr_attempted": bool(ocr_attempted),
                    "quality_flag": "medium" if val_num is not None else "low",
                    "raw_line": line,
                    "pdf_checksum": checksum,
                    "citations": json.dumps(citations, ensure_ascii=False),
                    "report_year": year,
                    "report_year_start": _year_start(year),
                    "lineage_output_file": source_meta.get("lineage_output_file", ""),
                }
            )

    if not out:
        for line in lines:
            if len(line) <= 260 and any(ch.isalpha() for ch in line) and not line.upper().startswith("TOTAL") and not _is_noise_line(line):
                row_index = int(row_no)
                row_no += 1
                out.append(
                    {
                        **source_meta,
                        "row_no": row_index,
                        "col_no": 0,
                        "page_no": page_no,
                        "table_id": f"p{page_no:04d}:text",
                        "record_type": "toc_row",
                        "metric_category": "official_measured",
                        "metric_name": "text_line",
                        "metric_value_numeric": None,
                        "metric_value_text": line,
                        "metric_unit": None,
                        "extraction_method": "text",
                        "parser_name": "heuristic",
                        "parser_metadata": json.dumps({"line_type": "text_fallback"}, ensure_ascii=False),
                        "extraction_confidence": 0.35,
                        "ocr_attempted": bool(ocr_attempted),
                        "quality_flag": "low",
                        "raw_line": line,
                        "pdf_checksum": checksum,
                        "citations": json.dumps(citations, ensure_ascii=False),
                        "report_year": year,
                        "report_year_start": _year_start(year),
                        "lineage_output_file": source_meta.get("lineage_output_file", ""),
                    }
                )
                if row_no >= 5:
                    break

    return out, row_no


def _build_base_meta(source_row: pd.Series, url: str, checksum: str, parsed_year: str, started: str) -> dict[str, Any]:
    source_id = str(source_row.get("source_id", "nhai_annual_report_documents"))
    source_type = str(source_row.get("source_type", "official_measured"))
    source_title = str(source_row.get("dataset_source", source_row.get("dataset_title", "NHAI annual report")))
    return {
        "source_document_url": url,
        "source_document_title": str(source_row.get("document_title", "")),
        "source_document_sha256": checksum,
        "source_id": source_id,
        "source_type": source_type,
        "dataset_source": source_title,
        "dataset_created_at": started,
        "lineage_dataset_created_at": started,
        "lineage_output_file": "",
        "lineage_document_url": url,
        "lineage_document_title": str(source_row.get("document_title", "")),
        "lineage_source_id": source_id,
        "lineage_source_type": source_type,
        "lineage_report_year": parsed_year,
        "lineage_pdf_checksum": checksum,
        "report_year": parsed_year,
        "report_year_start": _year_start(parsed_year),
    }


def _extract_rows_for_pdf(url: str, source_row: pd.Series, output_root: Path) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc).isoformat()
    parser_metrics: dict[str, int] = defaultdict(int)
    parser_env = _parser_environment()

    payload, download_error = _download_pdf(url)
    if payload is None:
        return [
            {
                "source_document_url": url,
                "source_document_title": str(source_row.get("document_title", "")),
                "source_document_sha256": "",
                "source_id": str(source_row.get("source_id", "nhai_annual_report_documents")),
                "source_type": str(source_row.get("source_type", "official_measured")),
                "dataset_source": str(source_row.get("dataset_source", source_row.get("dataset_title", "NHAI annual report"))),
                "dataset_created_at": now,
                "lineage_dataset_created_at": now,
                "lineage_output_file": "",
                "lineage_document_url": url,
                "lineage_document_title": str(source_row.get("document_title", "")),
                "lineage_source_id": str(source_row.get("source_id", "nhai_annual_report_documents")),
                "lineage_source_type": str(source_row.get("source_type", "official_measured")),
                "lineage_report_year": _normalize_year(source_row.get("financial_year")),
                "lineage_pdf_checksum": "",
                "page_no": 0,
                "table_id": "error",
                "row_no": 0,
                "col_no": 0,
                "row_index": 0,
                "record_type": "extraction_error",
                "metric_category": "official_measured",
                "metric_name": "document_download_failed",
                "metric_value_numeric": 0.0,
                "metric_value_text": str(download_error),
                "metric_unit": "binary",
                "extraction_method": "error",
                "parser_name": "network",
                "parser_metadata": _json_text(
                    {
                        "error": "download_failed",
                        "parser_environment": parser_env,
                        "parsers_attempted": [],
                        "attempt_count": 0,
                        "first_success_parser": "none",
                        "parser_failure_reason": download_error,
                    }
                ),
                "extraction_confidence": 0.0,
                "ocr_attempted": False,
                "quality_flag": "failed",
                "raw_line": str(download_error),
                "pdf_checksum": "",
                "citations": json.dumps({"document_url": url}, ensure_ascii=False),
                "report_year": _normalize_year(source_row.get("financial_year")),
                "report_year_start": _year_start(_normalize_year(source_row.get("financial_year"))),
            }
        ]

    checksum = _checksum(payload)
    parsed_year = _normalize_year(source_row.get("financial_year", source_row.get("document_title")))
    base_meta = _build_base_meta(source_row, url, checksum, parsed_year, now)
    citations_base = {
        "document_url": url,
        "source_year": parsed_year,
    }

    rows: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="nhai_ar_extract_", dir=str(output_root)) as d:
        pdf_path = Path(d) / "source.pdf"
        pdf_path.write_bytes(payload)

        full_text = _extract_text_pdftotext(pdf_path)
        page_texts = _extract_text_pdfreader(pdf_path)

        if not full_text.strip() and page_texts:
            full_text = "\x0c".join(page_texts)

        pages = full_text.split("\x0c") if full_text else [""] * max(1, len(page_texts))
        if page_texts and len(pages) != len(page_texts):
            pages = page_texts

        row_no = 0
        try:
            reader = PdfReader(str(pdf_path))
            total_pages = len(reader.pages)
        except Exception:
            total_pages = len(pages)
        total_pages = min(total_pages, MAX_PDF_PAGES)

        for idx in range(1, max(1, total_pages) + 1):
            page_no = idx
            page_text = page_texts[idx - 1] if idx - 1 < len(page_texts) else (pages[idx - 1] if idx - 1 < len(pages) else "")
            citations = {**citations_base, "anchor": f"page_{page_no}"}
            parsers_attempted: list[str] = []
            first_success_parser = "none"
            parser_failure_reason: str | None = None

            # Priority: structured table extraction
            parsed_by_table = False
            table_rows: list[dict[str, Any]] = []
            table_id = f"p{page_no:04d}"
            should_try_table = _page_has_table_signal(page_text)

            if should_try_table:
                parsers_attempted.append("camelot" if HAS_CAMELOT else "camelot_unavailable")
                table_data = _extract_with_camelot(pdf_path, page_no)
                if table_data:
                    parser_metrics["camelot"] += 1
                    first_success_parser = "camelot"
                    for local_row_no, row in enumerate(table_data, start=1):
                        table_rows.extend(
                            _table_row_to_extracted(
                                page_no=page_no,
                                table_id=table_id,
                                row=row,
                                row_no=row_no + local_row_no,
                                source_meta=base_meta,
                                checksum=checksum,
                                parser_name="camelot",
                                extraction_method="table",
                                year=parsed_year,
                                citations={**citations, "parser": "camelot"},
                                ocr_attempted=False,
                                used_parser=True,
                            )
                        )
                    parsed_by_table = True
            else:
                parsers_attempted.append("table_signal_not_detected")

            if not parsed_by_table:
                parsers_attempted.append("pdfplumber" if HAS_PDFPLUMBER else "pdfplumber_unavailable")
                table_data = _extract_with_pdfplumber(pdf_path, page_no) if should_try_table else []
                if table_data:
                    parser_metrics["pdfplumber"] += 1
                    first_success_parser = "pdfplumber"
                    for local_row_no, row in enumerate(table_data, start=1):
                        table_rows.extend(
                            _table_row_to_extracted(
                                page_no=page_no,
                                table_id=table_id,
                                row=row,
                                row_no=row_no + local_row_no,
                                source_meta=base_meta,
                                checksum=checksum,
                                parser_name="pdfplumber",
                                extraction_method="table",
                                year=parsed_year,
                                citations={**citations, "parser": "pdfplumber"},
                                ocr_attempted=False,
                                used_parser=True,
                            )
                        )
                    parsed_by_table = True

            if not parsed_by_table:
                parsers_attempted.append("tabula" if HAS_TABULA else "tabula_unavailable")
                table_data = _extract_with_tabula(pdf_path, page_no) if should_try_table else []
                if table_data:
                    parser_metrics["tabula"] += 1
                    first_success_parser = "tabula"
                    for local_row_no, row in enumerate(table_data, start=1):
                        table_rows.extend(
                            _table_row_to_extracted(
                                page_no=page_no,
                                table_id=table_id,
                                row=row,
                                row_no=row_no + local_row_no,
                                source_meta=base_meta,
                                checksum=checksum,
                                parser_name="tabula",
                                extraction_method="table",
                                year=parsed_year,
                                citations={**citations, "parser": "tabula"},
                                ocr_attempted=False,
                                used_parser=True,
                            )
                        )
                    parsed_by_table = True

            if table_rows:
                table_rows = _apply_parser_trace(
                    table_rows,
                    parser_env=parser_env,
                    parsers_attempted=parsers_attempted,
                    first_success_parser=first_success_parser,
                    parser_failure_reason=None,
                )
                table_rows = _dedupe_rows(table_rows)
                row_no += len(table_rows)
                rows.extend(table_rows)
                continue

            ocr_used = False
            usable_text = page_text
            parsers_attempted.append("text")
            if not page_text.strip() or len(re.findall(r"\w", page_text)) < 50:
                parsers_attempted.append("ocr" if parser_env.get("tesseract", False) else "ocr_unavailable")
                ocr_text = _ocr_page(pdf_path, page_no)
                if ocr_text.strip():
                    usable_text = ocr_text
                    ocr_used = True
                    parser_metrics["ocr"] += 1
                    first_success_parser = "ocr"
                else:
                    parser_metrics["text"] += 1
                    first_success_parser = "text"
                    parser_failure_reason = "ocr_not_usable"
            else:
                parser_metrics["text"] += 1
                first_success_parser = "text"

            extracted_text_rows, row_no = _line_text_rows(
                page_no=page_no,
                page_text=usable_text,
                row_no=row_no,
                source_meta=base_meta,
                checksum=checksum,
                year=parsed_year,
                citations=citations,
                ocr_attempted=ocr_used,
            )

            for row in extracted_text_rows:
                if ocr_used:
                    row["parser_name"] = "ocr"
                    row["extraction_method"] = "ocr"
                else:
                    row["parser_name"] = "text"
            extracted_text_rows = _apply_parser_trace(
                extracted_text_rows,
                parser_env=parser_env,
                parsers_attempted=parsers_attempted,
                first_success_parser=first_success_parser,
                parser_failure_reason=parser_failure_reason,
            )
            extracted_text_rows = _dedupe_rows(extracted_text_rows)
            rows.extend(extracted_text_rows)

    if not rows:
        rows = [
            {
                **base_meta,
                "page_no": 0,
                "row_no": 0,
                "col_no": 0,
                "row_index": 0,
                "table_id": "empty",
                "record_type": "no_rows",
                "metric_category": "official_measured",
                "metric_name": "no_extractable_rows",
                "metric_value_numeric": 0.0,
                "metric_value_text": "no_rows",
                "metric_unit": "binary",
                "extraction_method": "failed",
                "parser_name": "none",
                "parser_metadata": _json_text(
                    {
                        "attempted": parser_metrics,
                        "parser_environment": parser_env,
                        "parsers_attempted": list(parser_metrics.keys()),
                        "attempt_count": len(parser_metrics),
                        "first_success_parser": "none",
                        "parser_failure_reason": "no_extractable_rows",
                    }
                ),
                "extraction_confidence": 0.0,
                "ocr_attempted": False,
                "quality_flag": "failed",
                "raw_line": "No extractable rows from any parser.",
                "pdf_checksum": checksum,
                "citations": json.dumps({"document_url": url}, ensure_ascii=False),
                "report_year": parsed_year,
                "report_year_start": _year_start(parsed_year),
            }
        ]

    # finalize index fields
    for idx, row in enumerate(rows):
        row["row_index"] = idx

    return rows


def _coerce_frame(df: pd.DataFrame, default_file: Path | None = None) -> pd.DataFrame:
    out = df.copy()
    if "page_number" in out.columns and "page_no" not in out.columns:
        out["page_no"] = out["page_number"]
    if "document_title" in out.columns and "source_document_title" not in out.columns:
        out["source_document_title"] = out["document_title"]
    if "document_url" in out.columns and "source_document_url" not in out.columns:
        out["source_document_url"] = out["document_url"]
    if "unit" in out.columns and "metric_unit" not in out.columns:
        out["metric_unit"] = out["unit"]
    if default_file is not None:
        out["lineage_output_file"] = str(default_file)
    for col in CANONICAL_COLUMNS:
        if col not in out.columns:
            out[col] = None
    for col in ["row_index", "row_no", "col_no", "page_no", "extraction_confidence", "report_year_start", "metric_value_numeric"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out["ocr_attempted"] = out["ocr_attempted"].fillna(False).astype(bool)
    out = out.loc[:, ~out.columns.duplicated()]
    return out[CANONICAL_COLUMNS]


def _coerce_year(value: str) -> str:
    return _normalize_year(value)


def _filter_annual_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    mask = df["document_title"].fillna("").str.contains("Annual Report", case=False, na=False)
    mask |= df["source_document_url"].fillna("").str.contains("Annual_Report|annual-report|annual_report", case=False, regex=True, na=False)
    return df.loc[mask].copy()


def _source_payload(source_row: pd.Series, doc_index: int) -> dict[str, Any]:
    payload = source_row.to_dict()
    payload["doc_index"] = doc_index
    return payload


def _error_result(payload: dict[str, Any], error_text: str) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    url = str(payload.get("source_document_url", "")).strip()
    parsed_year = _normalize_year(payload.get("financial_year", payload.get("document_title")))
    return {
        "doc_index": int(payload.get("doc_index", 0)),
        "year": parsed_year,
        "source_document_url": url,
        "source_document_title": str(payload.get("document_title", "")),
        "rows": [
            {
                "source_document_url": url,
                "source_document_title": str(payload.get("document_title", "")),
                "source_document_sha256": "",
                "source_id": str(payload.get("source_id", "nhai_annual_report_documents")),
                "source_type": str(payload.get("source_type", "official_measured")),
                "dataset_source": str(payload.get("dataset_source", payload.get("dataset_title", "NHAI annual report"))),
                "dataset_created_at": now,
                "lineage_dataset_created_at": now,
                "lineage_output_file": "",
                "lineage_document_url": url,
                "lineage_document_title": str(payload.get("document_title", "")),
                "lineage_source_id": str(payload.get("source_id", "nhai_annual_report_documents")),
                "lineage_source_type": str(payload.get("source_type", "official_measured")),
                "lineage_report_year": parsed_year,
                "lineage_pdf_checksum": "",
                "page_no": 0,
                "table_id": "worker_error",
                "row_no": 0,
                "col_no": 0,
                "row_index": 0,
                "record_type": "extraction_error",
                "metric_category": "official_measured",
                "metric_name": "document_extraction_failed",
                "metric_value_numeric": 0.0,
                "metric_value_text": error_text,
                "metric_unit": "binary",
                "extraction_method": "error",
                "parser_name": "worker",
                "parser_metadata": _json_text(
                    {
                        "error": "worker_failed",
                        "parser_environment": _parser_environment(),
                        "parsers_attempted": [],
                        "attempt_count": 0,
                        "first_success_parser": "none",
                        "parser_failure_reason": error_text,
                    }
                ),
                "extraction_confidence": 0.0,
                "ocr_attempted": False,
                "quality_flag": "failed",
                "raw_line": error_text,
                "pdf_checksum": "",
                "citations": json.dumps({"document_url": url}, ensure_ascii=False),
                "report_year": parsed_year,
                "report_year_start": _year_start(parsed_year),
            }
        ],
        "error": error_text,
    }


def _extract_one_document(payload: dict[str, Any], output_root: str) -> dict[str, Any]:
    url = str(payload.get("source_document_url", "")).strip()
    parsed_year = _normalize_year(payload.get("financial_year", payload.get("document_title")))
    try:
        rows = _extract_rows_for_pdf(url, payload, Path(output_root))
    except Exception as exc:
        return _error_result(payload, f"worker_exception:{exc}")
    return {
        "doc_index": int(payload.get("doc_index", 0)),
        "year": parsed_year,
        "source_document_url": url,
        "source_document_title": str(payload.get("document_title", "")),
        "rows": rows,
        "error": None,
    }


def build_canonical(yearly_root: Path, canonical_path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    yearly_files = sorted((yearly_root / "yearly").glob("nhai_annual_report_*.parquet"))
    if not yearly_files:
        return pd.DataFrame(), {
            "status": "no_yearly_datasets",
            "total": 0,
            "merged": 0,
            "skipped": [],
            "canonical_shape": [],
            "schema_compatibility": {"reference_shape": [], "details": []},
        }

    shape_counts: Counter[tuple] = Counter()
    file_records = []

    for path in yearly_files:
        try:
            df = _coerce_frame(pd.read_parquet(path), default_file=path)
        except Exception as exc:
            file_records.append({"path": str(path), "status": "schema_read_failed", "reason": str(exc), "rows": 0, "schema": []})
            continue

        sig = tuple(sorted(df.columns))
        shape_counts[sig] += 1
        file_records.append({"path": str(path), "status": "read", "rows": int(len(df)), "schema": list(sig)})

    if not shape_counts:
        return pd.DataFrame(), {
            "status": "no_valid_yearly_files",
            "total": len(file_records),
            "merged": 0,
            "skipped": file_records,
            "canonical_shape": [],
            "schema_compatibility": {"reference_shape": [], "details": file_records},
        }

    reference_shape = shape_counts.most_common(1)[0][0]
    rows = []
    skipped = []
    for entry in file_records:
        path = Path(entry["path"])
        try:
            df = _coerce_frame(pd.read_parquet(path), default_file=path)
            if df.empty:
                skipped.append({"path": path.name, "status": "empty_dataset", "reason": "no_rows", "rows": 0})
                continue
            rows.append(df)
        except Exception as exc:
            skipped.append({"path": path.name, "status": "read_error", "reason": str(exc), "rows": entry["rows"]})

    merged = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    if not merged.empty:
        merged.to_parquet(canonical_path, index=False)

    return merged, {
        "status": "ok" if not merged.empty else "empty",
        "total": len(yearly_files),
        "merged": len(rows),
        "skipped": skipped,
        "canonical_shape": list(reference_shape),
        "schema_compatibility": {
            "reference_shape": list(reference_shape),
            "shape_counts": {"|".join(shape): count for shape, count in shape_counts.items()},
            "details": file_records,
        },
    }


def build_quality_report(
    rows: pd.DataFrame,
    canonical_summary: dict[str, Any],
    yearly_manifest: dict[str, Any],
    output_path: Path,
    parser_environment: dict[str, bool],
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_rows": int(len(rows)),
        "canonical_rows": int(len(rows)),
        "parser_environment": parser_environment,
        "method_mix": {},
        "quality": {
            "avg_confidence": 0.0,
            "min_confidence": None,
            "max_confidence": None,
            "low_confidence_rows": 0,
            "low_confidence_threshold": 0.45,
        },
        "yearly_counts": {},
        "schema": canonical_summary.get("schema_compatibility", {}),
        "parser_usage": {},
        "quality_flags": {},
        "summary": canonical_summary,
    }

    if rows.empty:
        _safe_path(output_path)
        output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        return report

    if "extraction_method" in rows.columns:
        method_counts = rows["extraction_method"].value_counts(dropna=False).to_dict()
        total = max(1, int(len(rows)))
        report["method_mix"] = {k: {"count": int(v), "share": float(v) / total} for k, v in method_counts.items()}

    if "parser_name" in rows.columns:
        for p, c in rows["parser_name"].value_counts(dropna=False).to_dict().items():
            report["parser_usage"][str(p)] = int(c)
    if "quality_flag" in rows.columns:
        for flag, count in rows["quality_flag"].value_counts(dropna=False).to_dict().items():
            report["quality_flags"][str(flag)] = int(count)

    conf = pd.to_numeric(rows["extraction_confidence"], errors="coerce").fillna(0)
    report["quality"]["avg_confidence"] = float(conf.mean()) if len(conf) else 0.0
    report["quality"]["min_confidence"] = float(conf.min()) if len(conf) else None
    report["quality"]["max_confidence"] = float(conf.max()) if len(conf) else None
    low = int((conf < 0.45).sum())
    report["quality"]["low_confidence_rows"] = low

    ycounts = rows.groupby("report_year").size().sort_index()
    report["yearly_counts"] = {str(k): int(v) for k, v in ycounts.to_dict().items()}
    report["yearly_dataset_summary"] = {
        y: {"rows": d.get("rows", 0), "status": d.get("status", "processed")}
        for y, d in yearly_manifest.items()
    }

    low_examples = []
    if not rows.empty:
        low_rows = rows.loc[rows["extraction_confidence"].fillna(0) < 0.45].head(30)
        for _, row in low_rows.iterrows():
            low_examples.append(
                {
                    "report_year": row.get("report_year"),
                    "source_document_url": row.get("source_document_url"),
                    "source_document_title": row.get("source_document_title"),
                    "extraction_method": row.get("extraction_method"),
                    "parser_name": row.get("parser_name"),
                    "extraction_confidence": row.get("extraction_confidence"),
                    "metric_name": row.get("metric_name"),
                    "raw_line": str(row.get("raw_line"))[:400],
                }
            )
    report["low_confidence_examples"] = low_examples

    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    _safe_path(path)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="NHAI annual report multi-agent-style extractor: table-first + OCR fallback and canonical merge"
    )
    parser.add_argument("--source-parquet", default="data/processed/nhai_annual_report_documents.parquet")
    parser.add_argument("--output-root", default="data/processed/nhai_annual_report_tables")
    parser.add_argument("--canonical-output", default="data/processed/nhai_annual_report_tables_canonical.parquet")
    parser.add_argument("--quality-report-output", default="data/processed/nhai_annual_report_tables/quality_report.json")
    parser.add_argument("--max-workers", type=int, default=1)
    args = parser.parse_args()

    source_path = Path(args.source_parquet)
    output_root = Path(args.output_root)
    canonical_path = Path(args.canonical_output)
    quality_path = Path(args.quality_report_output)

    yearly_root = output_root / "yearly"
    _safe_path(canonical_path)
    _safe_path(yearly_root)

    source_df = pd.read_parquet(source_path)
    annual_rows = _filter_annual_rows(source_df)
    if annual_rows.empty:
        print("No annual report rows found in source parquet.")
        return

    annual_rows = annual_rows.drop_duplicates(subset=["source_document_url", "financial_year", "document_title"]).copy()

    manifest: dict[str, Any] = {
        "source_parquet": str(source_path),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rows_input": int(len(annual_rows)),
        "parser_environment": _parser_environment(),
        "parallel_workers": 1,
        "parallel_order_strategy": "doc_index",
        "parallel_errors": [],
        "yearly_datasets": {},
        "parser_metrics": {},
        "canonical": {},
        "quality_report": str(quality_path),
    }

    payloads = [
        _source_payload(source_row, doc_index)
        for doc_index, (_, source_row) in enumerate(annual_rows.iterrows())
        if str(source_row.get("source_document_url", "")).strip()
    ]
    effective_workers = max(1, min(int(args.max_workers or 1), len(payloads) or 1, os.cpu_count() or 1))
    manifest["parallel_workers"] = effective_workers

    results: list[dict[str, Any]] = []
    if effective_workers == 1:
        for payload in payloads:
            results.append(_extract_one_document(payload, str(output_root)))
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=effective_workers) as executor:
            futures = [executor.submit(_extract_one_document, payload, str(output_root)) for payload in payloads]
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())

    results.sort(key=lambda item: (int(item.get("doc_index", 0)), str(item.get("year", "")), str(item.get("source_document_url", ""))))

    all_frames: list[pd.DataFrame] = []
    for result in results:
        year = str(result.get("year", "unknown"))
        url = str(result.get("source_document_url", "")).strip()
        rows = list(result.get("rows", []))
        row_df = pd.DataFrame(rows)
        row_df = _coerce_frame(row_df)
        row_df["metric_category"] = row_df["metric_category"].fillna("official_measured")
        if row_df["row_index"].isna().all():
            row_df["row_index"] = range(len(row_df))

        out_path = yearly_root / f"nhai_annual_report_{year}.parquet"
        row_df["lineage_output_file"] = str(out_path)
        row_df.to_parquet(out_path, index=False)

        manifest["yearly_datasets"][year] = {
            "source_document_url": url,
            "source_document_title": str(result.get("source_document_title", "")),
            "output_path": str(out_path),
            "rows": int(len(row_df)),
            "schema": [str(c) for c in row_df.columns],
            "first_records": int((row_df["record_type"] == "table_row").sum()),
            "method_mix": row_df["extraction_method"].value_counts(dropna=False).to_dict(),
            "quality": int((pd.to_numeric(row_df["extraction_confidence"], errors="coerce").fillna(0) < 0.45).sum()),
        }
        if result.get("error"):
            manifest["parallel_errors"].append(
                {
                    "doc_index": int(result.get("doc_index", 0)),
                    "year": year,
                    "source_document_url": url,
                    "error": str(result.get("error")),
                }
            )
        all_frames.append(row_df)

    # canonicalization across yearly files (schema compatibility)
    canonical_df, canonical_summary = build_canonical(output_root, canonical_path)
    if not canonical_df.empty:
        canonical_df = canonical_df.copy()
        for col in CANONICAL_COLUMNS:
            if col not in canonical_df.columns:
                canonical_df[col] = None
        canonical_df = canonical_df[CANONICAL_COLUMNS]

    all_df = pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame(columns=CANONICAL_COLUMNS)
    quality = build_quality_report(all_df, canonical_summary, manifest["yearly_datasets"], quality_path, manifest["parser_environment"])

    manifest["canonical"] = canonical_summary
    manifest["quality"] = quality["quality"]
    manifest["rows_merged"] = int(len(canonical_df)) if not canonical_df.empty else 0
    manifest["generated_at"] = datetime.now(timezone.utc).isoformat()

    _write_json(output_root / "extraction_manifest.json", manifest)

    print(f"Yearly datasets written to: {yearly_root}")
    print(f"Canonical dataset: {canonical_path} ({0 if canonical_df.empty else len(canonical_df)} rows)")
    print(f"Quality report: {quality_path}")
    print(f"Manifest: {output_root / 'extraction_manifest.json'}")


if __name__ == "__main__":
    main()
