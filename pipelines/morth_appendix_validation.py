from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

APPENDIX2_COUNT_METRIC = "appendix2_statewise_nh_count"
APPENDIX2_LENGTH_METRIC = "appendix2_statewise_nh_length_km"
APPENDIX2_METRICS = {APPENDIX2_COUNT_METRIC, APPENDIX2_LENGTH_METRIC}
EXPECTED_APPENDIX2_STATES = {
    "Andaman & Nicobar Islands",
    "Andhra Pradesh",
    "Arunachal Pradesh",
    "Assam",
    "Bihar",
    "Chandigarh",
    "Chhattisgarh",
    "Dadra & Nagar Haveli",
    "Daman & Diu",
    "Delhi",
    "Goa",
    "Gujarat",
    "Haryana",
    "Himachal Pradesh",
    "Jammu and Kashmir",
    "Jharkhand",
    "Karnataka",
    "Kerala",
    "Ladakh",
    "Madhya Pradesh",
    "Maharashtra",
    "Manipur",
    "Meghalaya",
    "Mizoram",
    "Nagaland",
    "Odisha",
    "Puducherry",
    "Punjab",
    "Rajasthan",
    "Sikkim",
    "Tamil Nadu",
    "Telangana",
    "Tripura",
    "Uttar Pradesh",
    "Uttarakhand",
    "West Bengal",
}
VALIDATION_REFERENCE_AGGREGATES = {
    "Dadra & Nagar Haveli": "Dadra & Nagar Haveli & Daman & Diu",
    "Daman & Diu": "Dadra & Nagar Haveli & Daman & Diu",
}
STATE_ALIASES = {
    "Orissa": "Odisha",
    "Uttrakhand": "Uttarakhand",
    "Uttaranchal": "Uttarakhand",
}


@dataclass
class Appendix2ValidationResult:
    errors: list[str]
    warnings: list[str]
    summary: dict[str, Any]


def normalize_appendix2_state(value: Any) -> str:
    text = " ".join(str(value or "").strip().split())
    return STATE_ALIASES.get(text, text)


def _appendix2_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "metric_name" not in df.columns:
        return pd.DataFrame()
    appendix = df[df["metric_name"].isin(APPENDIX2_METRICS)].copy()
    if appendix.empty:
        return appendix
    appendix["state"] = appendix["state"].map(normalize_appendix2_state)
    appendix["metric_value"] = pd.to_numeric(appendix["metric_value"], errors="coerce")
    appendix["source_as_of_date"] = appendix.get("source_as_of_date", "").fillna("").astype(str)
    return appendix


def _appendix2_wide(df: pd.DataFrame) -> pd.DataFrame:
    appendix = _appendix2_frame(df)
    if appendix.empty:
        return appendix
    grouped = appendix.groupby(["state", "metric_name"], as_index=False).agg(
        metric_value=("metric_value", "first"),
        source_as_of_date=("source_as_of_date", "first"),
    )
    wide = grouped.pivot(index="state", columns="metric_name", values="metric_value").reset_index()
    dates = grouped.groupby("state", as_index=False)["source_as_of_date"].first()
    wide = wide.merge(dates, on="state", how="left")
    return wide


def validate_appendix2_snapshot(df: pd.DataFrame) -> Appendix2ValidationResult:
    appendix = _appendix2_frame(df)
    errors: list[str] = []
    warnings: list[str] = []

    if appendix.empty:
        return Appendix2ValidationResult(errors=["Appendix 2 rows missing from MoRTH annual report dataset"], warnings=[], summary={})

    duplicates = appendix.groupby(["state", "metric_name"]).size()
    duplicate_rows = [f"{state}::{metric}" for (state, metric), count in duplicates.items() if count != 1]
    if duplicate_rows:
        errors.append(f"Appendix 2 has duplicate or missing pair rows: {', '.join(sorted(duplicate_rows))}")

    states_count = set(appendix[appendix["metric_name"] == APPENDIX2_COUNT_METRIC]["state"])
    states_length = set(appendix[appendix["metric_name"] == APPENDIX2_LENGTH_METRIC]["state"])
    missing_states = sorted(EXPECTED_APPENDIX2_STATES - states_count.union(states_length))
    extra_states = sorted(states_count.union(states_length) - EXPECTED_APPENDIX2_STATES)
    if missing_states:
        errors.append(f"Appendix 2 missing expected State/UT rows: {', '.join(missing_states)}")
    if extra_states:
        errors.append(f"Appendix 2 has unexpected State/UT rows: {', '.join(extra_states)}")
    if states_count != states_length:
        errors.append("Appendix 2 count and length coverage do not match by State/UT")

    wide = _appendix2_wide(df)
    if wide.empty:
        errors.append("Appendix 2 wide view could not be constructed")
        return Appendix2ValidationResult(errors=errors, warnings=warnings, summary={})

    source_dates = sorted({value for value in wide["source_as_of_date"].tolist() if value})
    if len(source_dates) != 1:
        errors.append(f"Appendix 2 should have one explicit source_as_of_date, found: {source_dates or ['<missing>']}")

    for row in wide.itertuples(index=False):
        state = getattr(row, "state")
        count = getattr(row, APPENDIX2_COUNT_METRIC, None)
        length = getattr(row, APPENDIX2_LENGTH_METRIC, None)
        if pd.isna(count) or pd.isna(length):
            errors.append(f"Appendix 2 missing paired values for {state}")
            continue
        if float(count) <= 0 or float(length) <= 0:
            errors.append(f"Appendix 2 has non-positive values for {state}")
            continue
        if abs(float(count) - round(float(count))) > 0.001:
            errors.append(f"Appendix 2 count is non-integer for {state}: {count}")
        if float(count) > 150:
            errors.append(f"Appendix 2 count is implausibly high for {state}: {count}")
        if float(length) > 25000:
            errors.append(f"Appendix 2 length is implausibly high for {state}: {length}")
        ratio = float(length) / max(float(count), 1.0)
        if ratio < 5 or ratio > 5000:
            errors.append(f"Appendix 2 count/length pairing is implausible for {state}: count={count}, length={length}")

    summary = {
        "state_count": int(len(wide)),
        "source_as_of_dates": source_dates,
        "total_length_km": float(wide[APPENDIX2_LENGTH_METRIC].sum()) if APPENDIX2_LENGTH_METRIC in wide else 0.0,
        "max_count_state": None,
    }
    if APPENDIX2_COUNT_METRIC in wide:
        max_row = wide.sort_values(APPENDIX2_COUNT_METRIC, ascending=False).iloc[0]
        summary["max_count_state"] = {
            "state": max_row["state"],
            "count": float(max_row[APPENDIX2_COUNT_METRIC]),
        }

    return Appendix2ValidationResult(errors=errors, warnings=warnings, summary=summary)


def compare_appendix2_to_reference(df: pd.DataFrame, reference_path: Path) -> dict[str, Any]:
    if not reference_path.exists():
        return {
            "reference_path": str(reference_path),
            "available": False,
            "errors": [f"Reference file missing: {reference_path}"],
            "warnings": [],
            "mismatches": [],
            "summary": {},
        }

    current = _appendix2_wide(df)
    reference = pd.read_csv(reference_path)
    reference["state"] = reference["state"].map(normalize_appendix2_state)

    current_rows: dict[str, dict[str, Any]] = {}
    for row in current.to_dict(orient="records"):
        state = row["state"]
        ref_state = VALIDATION_REFERENCE_AGGREGATES.get(state, state)
        bucket = current_rows.setdefault(ref_state, {"state": ref_state, "nh_count": 0.0, "nh_length_km": 0.0})
        bucket["nh_count"] += float(row.get(APPENDIX2_COUNT_METRIC, 0.0) or 0.0)
        bucket["nh_length_km"] += float(row.get(APPENDIX2_LENGTH_METRIC, 0.0) or 0.0)

    reference_rows = {
        row["state"]: {
            "nh_count": float(row["nh_count"]),
            "nh_length_km": float(row["nh_length_km"]),
            "source_as_of_date": str(row.get("source_as_of_date", "")),
            "citation_anchor": str(row.get("citation_anchor", "")),
            "source_url": str(row.get("source_url", "")),
        }
        for row in reference.to_dict(orient="records")
    }

    errors: list[str] = []
    warnings: list[str] = []
    mismatches: list[dict[str, Any]] = []
    shared_states = sorted(set(current_rows) & set(reference_rows))
    for state in shared_states:
        current_row = current_rows[state]
        reference_row = reference_rows[state]
        delta_count = current_row["nh_count"] - reference_row["nh_count"]
        delta_length = current_row["nh_length_km"] - reference_row["nh_length_km"]
        mismatch = {
            "state": state,
            "current_nh_count": current_row["nh_count"],
            "reference_nh_count": reference_row["nh_count"],
            "delta_nh_count": delta_count,
            "current_nh_length_km": current_row["nh_length_km"],
            "reference_nh_length_km": reference_row["nh_length_km"],
            "delta_nh_length_km": delta_length,
            "reference_source_as_of_date": reference_row["source_as_of_date"],
        }
        if delta_count or delta_length:
            mismatches.append(mismatch)
        if abs(delta_count) > max(10.0, reference_row["nh_count"] * 0.5):
            errors.append(f"Appendix 2 NH count drifts too far from parliamentary validation for {state}: current={current_row['nh_count']} ref={reference_row['nh_count']}")
        elif abs(delta_count) > 1:
            warnings.append(f"Appendix 2 NH count differs from parliamentary validation for {state}: current={current_row['nh_count']} ref={reference_row['nh_count']}")
        if abs(delta_length) > max(1000.0, reference_row["nh_length_km"] * 0.5):
            errors.append(f"Appendix 2 NH length drifts too far from parliamentary validation for {state}: current={current_row['nh_length_km']} ref={reference_row['nh_length_km']}")
        elif abs(delta_length) > 50:
            warnings.append(f"Appendix 2 NH length differs from parliamentary validation for {state}: current={current_row['nh_length_km']} ref={reference_row['nh_length_km']}")

    summary = {
        "shared_state_count": len(shared_states),
        "reference_state_count": len(reference_rows),
        "reference_source_as_of_dates": sorted({row["source_as_of_date"] for row in reference_rows.values() if row["source_as_of_date"]}),
    }
    return {
        "reference_path": str(reference_path),
        "available": True,
        "errors": errors,
        "warnings": warnings,
        "mismatches": mismatches,
        "summary": summary,
    }
