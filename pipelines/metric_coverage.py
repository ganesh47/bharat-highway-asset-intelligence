"""Metric periods, publication lag and quarter coverage are separate claims."""
from __future__ import annotations

import calendar
import re
from datetime import date

import pandas as pd

SCOPE = ["metric", "unit", "entity_type", "agency", "road_class", "period_basis", "statement_basis", "estimate_type", "price_basis", "base_year"]
HISTORIC_ACCIDENT_METRICS = {
    "data_gov_in_road_accidents_nhs_2003_2016": "road_accidents_nh",
    "data_gov_in_road_accidents_india_2003_2016": "road_accidents_all_roads",
    "data_gov_in_road_fatal_accidents_2003_2016": "fatal_crashes_all_roads",
}


def quarter_context(cutoff: str) -> dict:
    current = date.fromisoformat(cutoff)
    month = (current.month - 1) // 3 * 3 + 1
    start = date(current.year, month, 1)
    end_month = month + 2
    end = date(current.year, end_month, calendar.monthrange(current.year, end_month)[1])
    fiscal_start_year = current.year if current.month >= 4 else current.year - 1
    return {"calendar_quarter": f"{current.year}-Q{(current.month-1)//3+1}",
            "fiscal_quarter": f"FY{fiscal_start_year}-{str(fiscal_start_year+1)[2:]}-Q{((current.month-4)%12)//3+1}",
            "quarter_start": start.isoformat(), "quarter_end": end.isoformat()}


def _date(value):
    if value is None or pd.isna(value) or not str(value).strip():
        return None
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    return parsed.date().isoformat() if pd.notna(parsed) else None


def _period(value):
    fiscal = re.fullmatch(r"((?:19|20)\d{2})[-/](\d{2}|\d{4})", str(value).strip())
    if fiscal:
        end = int(fiscal[2]); end = end if end >= 100 else int(fiscal[1]) // 100 * 100 + end
        return f"{fiscal[1]}-04-01", f"{end}-03-31", "fiscal_year"
    if re.fullmatch(r"(?:19|20)\d{2}(?:\.0)?", str(value).strip()):
        year = int(float(value)); return f"{year}-01-01", f"{year}-12-31", "calendar_year"
    return "", "", "unknown"


def _legacy_rows(df, source, entry):
    sid = source["source_id"]
    scope = entry.get("analytical_scope", {})
    for original in df.to_dict("records"):
        entity = next((str(original[key]) for key in ["entity_id", "state", "state/ut", "states/ut", "states/uts"] if key in original and pd.notna(original[key])), "All India")
        base = {"entity_id": entity, "entity_type": scope.get("entity_type", source.get("entity_type", "unspecified")),
                "agency": scope.get("agency", source.get("publisher_org", "Unknown")),
                "road_class": scope.get("road_class", "unspecified"), "statement_basis": "source_specific_reported",
                "estimate_type": "actual", "observation_status": original.get("series_status", "reported"),
                "published_at": entry.get("publication_date"), "unit": original.get("unit", "source_specific"),
                "price_basis": "", "base_year": "", "data_as_of": None}
        if "metric_name" in original and "metric_value" in original:
            start, end, basis = _period(original.get("year", original.get("period", "")))
            if pd.isna(original.get("metric_value")):
                continue
            yield base | {"metric": original["metric_name"], "period_start": start, "period_end": end,
                          "period_basis": basis, "data_as_of": _date(original.get("source_as_of_date"))}
        elif sid == "nhai_constructed_length_series_official":
            start,end,basis = _period(original.get("period", ""))
            yield base | {"metric": "nhai_constructed_length_km", "period_start": start, "period_end": end,
                          "period_basis": basis, "data_as_of": _date(original.get("source_as_of_date"))}
        else:
            for key, value in original.items():
                if pd.isna(value):
                    continue
                match = re.search(r"((?:19|20)\d{2}(?:[-/]\d{2})?)$", key)
                metric = None
                if sid in HISTORIC_ACCIDENT_METRICS and re.fullmatch(r"\d{4}", key):
                    metric = HISTORIC_ACCIDENT_METRICS[sid]
                elif sid == "data_gov_in_nh_fatalities_injuries_state_year" and key.startswith(("fatalities", "injuries")):
                    metric = key.split("_")[0]
                elif sid == "data_gov_in_nhai_stateut_length_constructed_2019_24" and key.startswith("length_constructed"):
                    metric = "nhai_constructed_length_km"
                elif sid == "data_gov_in_gsdp_stateut_current_prices_2017_23" and key.startswith("gross_state_domestic_product"):
                    metric = "gsdp_current_prices" if "current_prices" in key else "gsdp_constant_prices"
                if metric and match:
                    try:
                        float(value)
                    except (TypeError, ValueError):
                        continue
                    start,end,basis = _period(match[1])
                    yield base | {"metric": metric, "period_start": start, "period_end": end, "period_basis": basis,
                                  "data_as_of": end, "price_basis": "current_prices" if "current_prices" in metric else "constant_prices" if "constant_prices" in metric else "",
                                  "base_year": "2011-12" if "constant_prices" in metric else ""}


def metric_coverage(df: pd.DataFrame, source: dict, entry: dict, cutoff: str) -> list[dict]:
    context = quarter_context(cutoff)
    supported = entry.get("evidence_status") not in {"unverified", "manual_unverified", "unavailable", "synthetic", "document_metadata"}
    if supported and {"metric", "value", "data_as_of", "source_document_sha256"} <= set(df):
        records = df.to_dict("records")
    elif supported:
        records = list(_legacy_rows(df,source,entry))
    else:
        records = []
    if not records:
        return [{"source_id": source["source_id"], "metric": "dataset", "coverage_status": "date_unknown" if supported else "not_calculation_ready",
                 "latest_observation_date": entry.get("source_as_of_date") if supported else None,
                 "latest_publication_date": entry.get("publication_date"), "next_expected_publication_at": source.get("next_expected_publication_at"),
                 "current_quarter_coverage": "not_applicable", "row_count": len(df), **context}]
    frame = pd.DataFrame(records)
    for key in SCOPE:
        if key not in frame:
            frame[key] = ""
        frame[key] = frame[key].fillna("")
    result = []
    for keys, group in frame.groupby(SCOPE,dropna=False,sort=True):
        scope = dict(zip(SCOPE,keys)); items = group.to_dict("records")
        observations = sorted({_date(row.get("data_as_of")) for row in items} - {None})
        publications = sorted({_date(row.get("published_at")) for row in items} - {None})
        latest = observations[-1] if observations else None
        newest_rows = [row for row in items if _date(row.get("data_as_of")) == latest] if latest else items
        latest_observation_publications = sorted({_date(row.get("published_at")) for row in newest_rows} - {None})
        statuses = sorted({str(row.get("observation_status") or "reported") for row in newest_rows})
        periods = sorted({str(row.get("period_end") or "") for row in items} - {""})
        next_date = _date(source.get("next_expected_publication_at"))
        annual = scope["period_basis"] in {"fiscal_year", "calendar_year"}
        status = "date_unknown" if not latest else "annual_only" if annual else "latest_reported"
        if "provisional" in statuses:
            status = "official_provisional"
        if next_date and next_date > cutoff:
            status = "publication_pending"
        estimate = scope["estimate_type"] not in {"actual", "YTD"}
        if estimate:
            status = "estimate_vintage_known" if latest else "estimate_vintage_unknown"
        current = "not_applicable" if annual or estimate else "not_yet_reported"
        if latest and not annual and not estimate and latest >= context["quarter_start"]:
            current = "partial_reported"
            if scope["period_basis"] in {"calendar_quarter", "fiscal_quarter"} and latest == context["quarter_end"]:
                current = "complete_closed_quarter"
        complete_quarters = []
        if not estimate and scope["period_basis"] in {"calendar_month", "fiscal_month"}:
            by_entity = {}
            for row in items:
                end = _date(row.get("period_end")); start = _date(row.get("period_start"))
                if not end or not start or _date(row.get("data_as_of")) != end:
                    continue
                parsed = date.fromisoformat(end)
                if start != f"{end[:7]}-01" or parsed.day != calendar.monthrange(parsed.year, parsed.month)[1]:
                    continue
                entity = str(row.get("entity_id", "All India"))
                by_entity.setdefault(entity, []).append(end)
            for months in by_entity.values():
                for end in set(months):
                    parsed = date.fromisoformat(end)
                    if parsed.month % 3 or end > cutoff:
                        continue
                    required = [date(parsed.year, month, calendar.monthrange(parsed.year,month)[1]).isoformat()
                                for month in range(parsed.month-2,parsed.month+1)]
                    if all(months.count(month) == 1 for month in required):
                        complete_quarters.append(end)
        elif not estimate and scope["period_basis"] in {"calendar_quarter", "fiscal_quarter"}:
            complete_quarters = [str(row["period_end"]) for row in items
                                 if row.get("period_end") and str(row["period_end"]) <= cutoff
                                 and str(row.get("data_as_of")) == str(row["period_end"])]
        if context["quarter_end"] in complete_quarters:
            current = "complete_closed_quarter"
        lags = [(date.fromisoformat(publication)-date.fromisoformat(observation)).days for row in newest_rows
                if (publication := _date(row.get("published_at"))) and (observation := _date(row.get("data_as_of"))) and publication >= observation]
        entity_cutoffs = {}
        for row in items:
            entity = str(row.get("entity_id", "All India")); observation = _date(row.get("data_as_of"))
            if entity not in entity_cutoffs or (observation and observation > (entity_cutoffs[entity] or "")):
                entity_cutoffs[entity] = observation
        result.append(scope | {"source_id": source["source_id"], "coverage_status": status,
                              "latest_observation_date": latest, "earliest_entity_cutoff": min(value for value in entity_cutoffs.values() if value) if any(entity_cutoffs.values()) else None,
                              "latest_available_period_end": periods[-1] if periods else None,
                              "latest_publication_date": latest_observation_publications[-1] if latest_observation_publications else None,
                              "latest_disclosure_publication_date": publications[-1] if publications else None,
                              "publication_lag_days": max(lags) if lags else None,
                              "observation_status": statuses, "next_expected_publication_at": next_date,
                              "latest_complete_quarter_end": max(complete_quarters) if complete_quarters else None,
                              "current_quarter_coverage": current, "entity_count": len(entity_cutoffs),
                              "entity_cutoffs": entity_cutoffs, "row_count": len(group), **context})
    return result
