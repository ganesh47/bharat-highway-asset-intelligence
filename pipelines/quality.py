from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Any

import pandas as pd

EXTRACTION_QUALITY_SOURCE_IDS = {"nhai_annual_report_documents"}

FREQ_TO_DAYS = {
    "daily": 1,
    "monthly": 31,
    "quarterly": 92,
    "annual": 365,
    "unknown": 365,
}

STATUS_CONFIDENCE = {
    "ok": 1.0,
    "automated": 1.0,
    "manual_ingest": 0.98,
    "stub_parsed": 0.78,
    "metadata_only": 0.62,
    "proxy_stub": 0.38,
    "stubs_disabled": 0.30,
    "disabled": 0.22,
    "stubbed_manual_gap": 0.34,
    "candidate_ready": 0.44,
}


def _status_factor(item: Dict[str, Any]) -> float:
    status = str(item.get("status", "")).lower()
    if status in STATUS_CONFIDENCE:
        return STATUS_CONFIDENCE[status]
    return 0.5


def _status_reason(status: str) -> str | None:
    if status in {"stubs_disabled", "disabled"}:
        return "Ingestion is disabled under manual approval gate."
    if status in {"stubbed_manual_gap", "candidate_ready"}:
        return "Connector is a controlled placeholder until a validated official endpoint/docs are available."
    if status == "proxy_stub":
        return "This signal is proxy-derived and not an official measurement."
    if status == "metadata_only":
        return "Metadata-only path; does not contain source metric values."
    return None


def completeness_score(df: pd.DataFrame) -> float:
    if df.empty:
        return 0.0
    total = len(df) * max(len(df.columns), 1)
    missing = int(df.isna().sum().sum())
    return round(max(0.0, 1 - (missing / total)), 3)


def recency_score(last_updated: str | None, update_frequency: str | None) -> float:
    if not last_updated:
        return 0.25
    try:
        parsed = datetime.fromisoformat(last_updated.replace("Z", "+00:00"))
    except Exception:
        return 0.25

    freq_days = FREQ_TO_DAYS.get((update_frequency or "unknown").lower(), 365)
    age = datetime.now(timezone.utc).replace(tzinfo=timezone.utc) - parsed
    age_days = age.total_seconds() / 86400

    if age_days <= freq_days:
        return 1.0
    if age_days <= freq_days * 2:
        return 0.7
    if age_days <= freq_days * 4:
        return 0.4
    return 0.2


def provenance_score(item: Dict[str, Any]) -> float:
    rel = str(item.get("reliability_grade", "C")).upper()
    if rel == "A" and item.get("official_flag"):
        return 1.0
    if rel == "B" and item.get("official_flag"):
        return 0.85
    if rel == "C" and not item.get("official_flag"):
        return 0.6
    if rel == "C":
        return 0.7
    return 0.5


def consistency_score(df: pd.DataFrame, numeric_nonnegative: list[str] | None = None) -> float:
    if df.empty:
        return 0.3

    score = 1.0
    numeric_nonnegative = numeric_nonnegative or []
    for col in numeric_nonnegative:
        if col not in df.columns:
            score -= 0.05
            continue
        if (pd.to_numeric(df[col], errors="coerce") < 0).any():
            score -= 0.25
    if score < 0:
        score = 0.0
    return round(min(score, 1.0), 3)


def _extract_extraction_quality(item: Dict[str, Any]) -> dict[str, Any]:
    source_id = str(item.get("source_id", "")).strip()
    if source_id not in EXTRACTION_QUALITY_SOURCE_IDS:
        return {}
    payload = item.get("extraction_quality")
    if not isinstance(payload, dict):
        return {}
    quality = payload.get("quality")
    method_mix = payload.get("method_mix")
    parser_environment = payload.get("parser_environment")
    if not isinstance(quality, dict) or not isinstance(method_mix, dict):
        return {}
    if parser_environment is not None and not isinstance(parser_environment, dict):
        parser_environment = {}
    return {
        "quality": quality,
        "method_mix": method_mix,
        "parser_environment": parser_environment or {},
    }


def _method_count(method_mix: dict[str, Any], name: str) -> int:
    raw = method_mix.get(name, 0)
    if isinstance(raw, dict):
        raw = raw.get("count", 0)
    try:
        return int(raw)
    except Exception:
        return 0


def _extraction_confidence_factor(summary: dict[str, Any]) -> tuple[float, list[str]]:
    if not summary:
        return 1.0, []

    quality = summary.get("quality") if isinstance(summary.get("quality"), dict) else {}
    method_mix = summary.get("method_mix") if isinstance(summary.get("method_mix"), dict) else {}
    parser_environment = (
        summary.get("parser_environment") if isinstance(summary.get("parser_environment"), dict) else {}
    )

    try:
        avg_conf = float(quality.get("avg_confidence", 1.0))
    except Exception:
        avg_conf = 1.0
    try:
        low_conf_rows = float(quality.get("low_confidence_rows", 0))
    except Exception:
        low_conf_rows = 0.0
    total_rows = 0.0
    for value in method_mix.values():
        if isinstance(value, dict):
            value = value.get("count", 0)
        try:
            total_rows += float(value)
        except Exception:
            continue

    low_conf_share = (low_conf_rows / total_rows) if total_rows > 0 else 0.0
    failed_rows = _method_count(method_mix, "failed") + _method_count(method_mix, "error")
    failed_share = (failed_rows / total_rows) if total_rows > 0 else 0.0
    text_rows = _method_count(method_mix, "text")
    text_share = (text_rows / total_rows) if total_rows > 0 else 0.0
    parser_support = any(bool(parser_environment.get(name)) for name in ("pdfplumber", "camelot", "tabula", "tesseract"))

    score = max(0.0, min(avg_conf, 1.0))
    reasons: list[str] = []

    if low_conf_share > 0.15:
        score -= min(0.25, (low_conf_share - 0.15) * 0.6)
        reasons.append("Extraction quality has a meaningful share of low-confidence rows")
    if failed_share > 0.02:
        score -= min(0.2, failed_share * 2.0)
        reasons.append("Extractor recorded failed/error rows")
    if text_share > 0.95 and not parser_support:
        score -= 0.08
        reasons.append("Extraction is dominated by text-only fallback without structured parser/OCR support")
    if avg_conf < 0.7:
        reasons.append("Average extraction confidence is below the preferred threshold")

    return round(max(0.0, min(score, 1.0)), 3), reasons


def confidence_badge(scores: Dict[str, float]) -> tuple[str, list[str]]:
    reasons = []
    has_extraction = "extraction" in scores
    if has_extraction:
        overall = (
            0.30 * scores.get("completeness", 0)
            + 0.20 * scores.get("recency", 0)
            + 0.18 * scores.get("provenance", 0)
            + 0.17 * scores.get("consistency", 0)
            + 0.15 * scores.get("extraction", 0)
        )
    else:
        overall = (
            0.35 * scores.get("completeness", 0)
            + 0.25 * scores.get("recency", 0)
            + 0.2 * scores.get("provenance", 0)
            + 0.2 * scores.get("consistency", 0)
        )

    if scores.get("provenance", 0) < 0.45:
        reasons.append("Low provenance confidence (source reliability category)")
    if scores.get("completeness", 0) < 0.6:
        reasons.append("Missingness is high; check source completeness")
    if scores.get("recency", 0) < 0.6:
        reasons.append("Recency is stale against claimed update frequency")
    if scores.get("consistency", 0) < 0.7:
        reasons.append("Schema/range checks showed potential consistency issues")
    if has_extraction and scores.get("extraction", 1.0) < 0.65:
        reasons.append("Extraction confidence is lower than discovery confidence; verify OCR/parser quality")

    if overall >= 0.85:
        return "High", reasons
    if overall >= 0.65:
        return "Med", reasons
    return "Low", reasons + ["Use for trend and risk ranking, not precise governance decisions"]


def evaluate(df, item: Dict[str, Any]) -> Dict[str, Any]:
    c = completeness_score(df)
    r = recency_score(item.get("retrieved_at"), item.get("update_frequency"))
    p = provenance_score(item)
    cs = consistency_score(df)
    extraction_summary = _extract_extraction_quality(item)
    extraction_score, extraction_reasons = _extraction_confidence_factor(extraction_summary)
    status_factor = _status_factor(item)
    status = str(item.get("status", "")).lower()

    c = min(c, status_factor if status_factor <= 1 else c)
    r = min(r, max(0.2, status_factor))
    p = p * max(0.25, status_factor)
    cs = min(cs, max(0.2, status_factor))

    reason_from_status = _status_reason(status)

    badge, reasons = confidence_badge(
        {
            "completeness": c,
            "recency": r,
            "provenance": p,
            "consistency": cs,
            **({"extraction": extraction_score} if extraction_summary else {}),
        }
    )

    if reason_from_status:
        reasons = reasons + [reason_from_status]
    if extraction_reasons:
        reasons = reasons + [reason for reason in extraction_reasons if reason not in reasons]

    output = {
        "completeness_score": c,
        "recency_score": r,
        "provenance_score": p,
        "consistency_score": cs,
        "overall_confidence_badge": badge,
        "overall_confidence_reason": reasons,
    }
    if extraction_summary:
        output["extraction_quality_score"] = extraction_score
    return output
