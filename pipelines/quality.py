from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Any
import re

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
    "generated": 0.5,
    "validated": 1.0,
    "gap": 0.25,
    "failed": 0.2,
    "quarantined": 0.2,
}

CURATED_MANUAL_SOURCES = {
    "morth_annual_report_pdf", "parliament_qa_nh_blackspots_state",
    "nhai_constructed_length_series_official",
}
DOCUMENT_SOURCES = {"nhai_annual_report_documents", "nhai_audited_results_pdf", "nhai_press_release_index"}


def observation_date(df: pd.DataFrame, item: Dict[str, Any]) -> str | None:
    """Observation dates stay independent of publication and download dates."""
    for key in ("source_as_of_date", "data_as_of", "as_of_date", "observation_date", "period_end"):
        value = item.get(key)
        if value:
            parsed = pd.to_datetime(value, utc=True, errors="coerce")
            if pd.notna(parsed):
                return parsed.date().isoformat()
        if key in df.columns:
            dates = pd.to_datetime(df[key], utc=True, errors="coerce").dropna()
            if not dates.empty:
                return dates.max().date().isoformat()
    years = []
    # Use observed periods only; claimed inventory coverage is not observation evidence.
    for col in ("period", "financial_year", "reporting_period", "year_wise", "year", "report_year"):
        if col in df.columns:
            for value in df[col].dropna().astype(str):
                fiscal = re.fullmatch(r"(?:FY\s*)?((?:19|20)\d{2})[-/](\d{2}|\d{4})(?:\s.*)?", value.strip())
                if fiscal:
                    end = int(fiscal.group(2))
                    if end < 100:
                        end = (int(fiscal.group(1)) // 100) * 100 + end
                    years.append(f"{end}-03-31")
                elif re.fullmatch(r"(?:19|20)\d{2}(?:\.0)?", value.strip()):
                    years.append(f"{int(float(value))}-12-31")
    for col in df.columns:
        # Wide tables have a year in the measured column name, not a row year.
        fiscal = re.findall(r"((?:19|20)\d{2})[-/](\d{2})", col)
        calendar = re.findall(r"(?:^|_)((?:19|20)\d{2})(?:$|_)", col)
        if fiscal:
            for start, end in fiscal:
                years.append(f"{(int(start) // 100) * 100 + int(end)}-03-31")
        elif calendar:
            years.extend(f"{year}-12-31" for year in calendar)
    return max(years) if years else None


def evidence_status(df: pd.DataFrame, source: Dict[str, Any], manifest: Dict[str, Any]) -> str:
    if manifest.get("metric_category") == "model_output":
        return "synthetic"
    if manifest.get("status") == "metadata_only" or source.get("source_id") in DOCUMENT_SOURCES:
        return "document_metadata"
    if df.empty or manifest.get("status") in {"gap", "manual_gap", "failed", "disabled", "stubs_disabled", "not_mapped", "stubbed_manual_gap"}:
        return "unavailable"
    declared = manifest.get("evidence_status")
    if declared in {"validated", "verified", "validated_primary", "validated_curated"}:
        return declared
    if manifest.get("status") == "manual_ingest":
        required = {"citation_anchor", "source_as_of_date", "document_section"}
        if source.get("source_id") not in CURATED_MANUAL_SOURCES:
            required.add("source_url")
        if not required <= set(df.columns):
            return "unverified"
        if df[list(required)].isna().any().any() or df[list(required)].astype(str).apply(lambda s: s.str.strip().eq("")).any().any():
            return "unverified"
        if "source_url" in required and not df["source_url"].astype(str).str.match(r"https?://").all():
            return "unverified"
        return "validated_curated"
    if manifest.get("status") in {"ok", "automated", "validated"} and manifest.get("manifest", {}).get("raw_files"):
        return "validated"
    return "unverified"


def semantic_errors(df: pd.DataFrame, source: Dict[str, Any]) -> list[str]:
    """Publication constraints; discovery success cannot override invalid measurements."""
    if df.empty:
        return []
    errors: list[str] = []
    contract = source.get("schema_contract") or {}
    required = contract.get("required_columns", source.get("required_columns", []))
    missing = set(required) - set(df.columns)
    if missing:
        errors.append(f"Missing required columns: {sorted(missing)}")
    nonnegative = set(contract.get("numeric_nonnegative", source.get("numeric_nonnegative", [])))
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]) and any(token in col.lower() for token in ("length", "cost", "expenditure", "fatalit", "killed", "injur", "accident", "blackspot", "km_constructed")):
            nonnegative.add(col)
    for col in sorted(nonnegative):
        if col not in df:
            errors.append(f"Missing nonnegative measure: {col}")
        elif (pd.to_numeric(df[col], errors="coerce") < 0).any():
            errors.append(f"Negative values in {col}")
    for col in df.columns:
        if "progress" in col.lower() and any(token in col.lower() for token in ("pct", "percent")):
            values = pd.to_numeric(df[col], errors="coerce")
            if ((values < 0) | (values > 100)).any():
                errors.append(f"Progress outside 0–100 in {col}")
    keys = contract.get("unique_key", [])
    if keys and set(keys) <= set(df.columns) and df.duplicated(subset=keys).any():
        errors.append(f"Duplicate observations for {keys}")
    if source.get("source_id") == "morth_annual_report_pdf":
        from pipelines.morth_appendix_validation import validate_appendix2_snapshot
        errors.extend(validate_appendix2_snapshot(df).errors)
    if source.get("source_id") == "nhai_constructed_length_series_official":
        if "series_scope" not in df or set(df["series_scope"].dropna()) != {"NHAI-only"}:
            errors.append("Construction series must remain NHAI-only")
        if "period" not in df or df["period"].duplicated().any():
            errors.append("Construction series requires unique periods")
    sid = source.get("source_id")
    if sid in {"data_gov_in_nh_fatalities_injuries_state_year", "data_gov_in_nhai_stateut_project_delay_status_2024", "data_gov_in_gsdp_stateut_current_prices_2017_23", "parliament_qa_nh_blackspots_state"}:
        state_col = next((col for col in ("states/ut", "state/ut", "state_ut", "state", "states_ut") if col in df), None)
        if state_col is None:
            errors.append("Missing State/UT identity column")
        else:
            core = df.loc[~df[state_col].astype(str).str.strip().str.lower().isin({"total", "india", "all india"})]
            minimum = 35 if sid == "data_gov_in_nh_fatalities_injuries_state_year" else 30
            if core[state_col].nunique() < minimum:
                errors.append(f"Insufficient State/UT coverage: expected at least {minimum}")
            if core[state_col].duplicated().any():
                errors.append("Duplicate State/UT observations")
            if sid == "data_gov_in_nh_fatalities_injuries_state_year":
                for metric in ("fatalities", "injuries"):
                    for year in (2020, 2021, 2022):
                        column = next((col for col in core if metric[:-3] in col.lower() and str(year) in col), None)
                        if column is None:
                            errors.append(f"Missing {metric} {year} observations")
                            continue
                        values = pd.to_numeric(core[column], errors="coerce")
                        missing_states = set(core.loc[values.isna(), state_col].astype(str))
                        allowed_missing = {"Ladakh"} if year == 2020 else set()
                        if missing_states - allowed_missing:
                            errors.append(f"Missing/non-numeric {metric} {year} for {sorted(missing_states - allowed_missing)}")
            if sid == "data_gov_in_nhai_stateut_project_delay_status_2024":
                for col in ("number_of_projects", "number_of_delayed_projects"):
                    if col not in core or pd.to_numeric(core[col], errors="coerce").isna().any():
                        errors.append(f"Missing/non-numeric {col}")
                if {"number_of_projects", "number_of_delayed_projects"} <= set(core) and (pd.to_numeric(core["number_of_delayed_projects"], errors="coerce") > pd.to_numeric(core["number_of_projects"], errors="coerce")).any():
                    errors.append("Delayed projects exceed reported total projects")
            if sid == "parliament_qa_nh_blackspots_state":
                for col in ("nh_blackspots", "nh_blackspot_accidents", "nh_blackspot_fatalities", "rectified_blackspots"):
                    if col not in core or pd.to_numeric(core[col], errors="coerce").isna().any():
                        errors.append(f"Missing/non-numeric {col}")
                if {"nh_blackspots", "rectified_blackspots"} <= set(core) and (pd.to_numeric(core["rectified_blackspots"], errors="coerce") > pd.to_numeric(core["nh_blackspots"], errors="coerce")).any():
                    errors.append("Rectified blackspots exceed reported blackspots")
            if sid == "data_gov_in_gsdp_stateut_current_prices_2017_23":
                columns = [col for col in core if "gross_state_domestic_product" in col and "current_prices" in col]
                if len(columns) < 6:
                    errors.append("Missing GSDP year coverage")
                elif core[columns].apply(lambda values: pd.to_numeric(values.astype(str).str.replace(",", "", regex=False), errors="coerce")).isna().all(axis=1).any():
                    errors.append("GSDP State/UT row has no numerical observation")
    if {"metric_name", "metric_value", "unit"} <= set(df.columns):
        values = pd.to_numeric(df["metric_value"], errors="coerce")
        if values.isna().any():
            errors.append("Non-numeric metric values")
        units = df["unit"].astype(str).str.strip().str.lower()
        if units.eq("km_per_km").any():
            errors.append("Unsupported roughness unit km_per_km")
        if df["metric_name"].astype(str).str.contains("million", case=False).any() and (df["metric_name"].astype(str).str.contains("million", case=False) & units.eq("count")).any():
            errors.append("Million-scale metric is labelled as an unscaled count")
    return list(dict.fromkeys(errors))


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
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return 0.25

    freq_days = FREQ_TO_DAYS.get((update_frequency or "unknown").lower(), 365)
    age = datetime.now(timezone.utc).replace(tzinfo=timezone.utc) - parsed
    age_days = age.total_seconds() / 86400
    if age_days < -1:
        return 0.25

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
    observed_at = observation_date(df, item)
    r = recency_score(observed_at, item.get("update_frequency"))
    p = provenance_score(item)
    failures = semantic_errors(df, item)
    cs = max(0.0, consistency_score(df, item.get("numeric_nonnegative")) - min(0.8, 0.25 * len(failures)))
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
    reasons.extend(failures)
    if not observed_at:
        reasons.append("Observation date is unavailable; collection time is not data freshness")
    if item.get("evidence_status") in {"unverified", "unavailable", "synthetic"} or failures:
        badge = "Low"
        reasons.append("Not eligible for measured analyst calculations")
    if item.get("extraction_status") in {"discovered", "fetched", "pending", "checksum_mismatch"}:
        badge = "Low"
        reasons.append("Document discovery is not validated financial extraction")

    output = {
        "completeness_score": c,
        "recency_score": r,
        "provenance_score": p,
        "consistency_score": cs,
        "overall_confidence_badge": badge,
        "overall_confidence_reason": reasons,
        "source_as_of_date": observed_at,
        "recency_basis": "observation_date" if observed_at else "unknown",
        "validation_errors": failures,
    }
    if extraction_summary:
        output["extraction_quality_score"] = extraction_score
    return output
