from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Any

import pandas as pd

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


def confidence_badge(scores: Dict[str, float]) -> tuple[str, list[str]]:
    reasons = []
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
        }
    )

    if reason_from_status:
        reasons = reasons + [reason_from_status]

    return {
        "completeness_score": c,
        "recency_score": r,
        "provenance_score": p,
        "consistency_score": cs,
        "overall_confidence_badge": badge,
        "overall_confidence_reason": reasons,
    }
