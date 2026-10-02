"""Reviewed national statistical and financial vintages, with original cutoffs.

Scanned tables are reproduced from a committed, visually reviewed transcription
bound to the exact publisher PDF. New OCR output cannot silently become facts.
The raw PDF, reviewed cells and CSV all have independent checksum lineage.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any

DOCUMENTS = {
    "morth_road_accidents_2024_final": "https://morth.gov.in/backend/documents/uploaded/1781177676_V1gUW8tJWT.pdf",
    "nhai_fy2025_26_performance": "https://www.pib.gov.in/PressReleasePage.aspx?PRID=2247870&lang=1&reg=3",
    "morth_annual_report_2025_26": "https://morth.gov.in/backend/documents/uploaded/RTH%20Annual%20Report%20English.pdf",
    "nhai_financial_results_2025_03_unaudited": "https://nhai.gov.in/nhai/sites/default/files/mix_file/Q_F_f_tQended_on_31-March-2025.pdf",
}
SOURCE_IDS = tuple(DOCUMENTS)
HTML_SOURCE_IDS = ("nhai_fy2025_26_performance",)
# PIB wrapper counters vary independently of the article. This snapshot uses
# a governed review until an article-only semantic recheck is configured.
HTML_RECHECK_SOURCE_IDS: tuple[str, ...] = ()
PINNED_SHA256 = {
    "morth_road_accidents_2024_final": "b45b0e4d8e14a8d4653790f9080a01b9b95b79008359ba93dc12497064be69b2",
    "nhai_fy2025_26_performance": "0f5de32b5f62870df66f4df9bb54b65262e6f89f296675f286e1a61852d0bfb3",
    "morth_annual_report_2025_26": "d0dd808600a8a2d0013b1fe4772d104f14dc18fc810c21a09a16aad01ea5624a",
    "nhai_financial_results_2025_03_unaudited": "d08837c303f72abf4d131e796f71eb93450be10cfdd96ee7fd4f921c3ac38c97",
}
REVIEWED_TABLE_FILES = {
    "morth_road_accidents_2024_final": "morth_road_accidents_2024_reviewed_tables.json",
    "morth_annual_report_2025_26": "morth_annual_report_2025_26_reviewed_tables.json",
    "nhai_financial_results_2025_03_unaudited": "nhai_financial_results_2025_03_reviewed_tables.json",
}
REVIEWED_TABLE_SHA256 = {
    "morth_road_accidents_2024_final": "8a54930c708f6b5c9e8912e51601c297100244e9ab81fad3f89b09e70f425384",
    "morth_annual_report_2025_26": "5a72990102ff4b75866747582b83c314f009e755ca4e0d090a2e9f1a91ec8f2c",
    "nhai_financial_results_2025_03_unaudited": "8900ae420de56dfe2f360a0b36135ffdc2e0e8c51faa5394f29639cbc4bac034",
}
ANNUAL_PUBLICATION = "2026-03-30"  # Exact official reports API record.
SAFETY_METRICS = {"road_accidents_count", "road_fatalities_count", "road_injuries_count", "fatal_road_accidents_count"}


def document_path(raw_root: Path, sid: str) -> Path:
    suffix = ".html" if sid == "nhai_fy2025_26_performance" else ".pdf"
    return raw_root / "primary_disclosures" / sid / ("document" + suffix)


def cell_number(cell: Any) -> float | None:
    """Printed zero is a value; a dash, NA or empty cell is missing."""
    text = str(cell).strip()
    if text in {"", "-", "–", "—", "NA", "N/A", "..."}:
        return None
    if not re.fullmatch(r"\(?-?\d[\d,]*(?:\.\d+)?\)?", text):
        raise ValueError(f"Unreviewed numeric cell: {text}")
    negative = text.startswith("(") and text.endswith(")")
    value = float(text.strip("()").replace(",", ""))
    return -value if negative else value


def reviewed_tables(raw_root: Path, sid: str) -> dict[str, Any]:
    path = raw_root / "manual" / "evidence" / REVIEWED_TABLE_FILES[sid]
    if hashlib.sha256(path.read_bytes()).hexdigest() != REVIEWED_TABLE_SHA256[sid]:
        raise ValueError("Reviewed table checksum changed; a new review is required")
    tables = json.loads(path.read_text())
    if tables.get("source_id") != sid or tables.get("document_sha256") != PINNED_SHA256[sid]:
        raise ValueError("Reviewed table provenance does not match the pinned publication")
    if not tables.get("review_method") or tables.get("reviewed_at") != "2026-10-02":
        raise ValueError("Missing original visual-review evidence")
    return tables


def validate_safety_tables(tables: dict[str, Any]) -> None:
    entries = tables["tables"]
    expected = {2, 3, 4, 5, 9, 10, 11, 12, 14, 15, 16, 17}
    if len(entries) != 12 or {t["annexure"] for t in entries} != expected:
        raise ValueError("Incomplete safety annexure set")
    identities = set()
    for table in entries:
        if table["years"] != [2021, 2022, 2023, 2024] or table["metric"] not in SAFETY_METRICS:
            raise ValueError("Safety year/metric contract changed")
        if len(table["rows"]) != 36 or len({r["state"] for r in table["rows"]}) != 36:
            raise ValueError("Safety state coverage changed")
        key = (table["road_class"], table["metric"])
        if key in identities:
            raise ValueError("Duplicate safety road-class table")
        identities.add(key)
        for row in table["rows"]:
            if len(row["original_cells"]) != 4:
                raise ValueError("Missing count cell cannot shift later years")
            values = [cell_number(x) for x in row["original_cells"]]
            if any(v is None or v < 0 or v != int(v) for v in values):
                raise ValueError("Safety counts require nonnegative printed integers")
        sums = [sum(cell_number(r["original_cells"][i]) for r in table["rows"]) for i in range(4)]
        if sums != table["published_total"] or sums != table["column_sum"]:
            raise ValueError("Safety state-column reconciliation failed")


def extract_performance(content: str) -> dict[str, float]:
    from pipelines.connectors.primary_disclosures import html_text
    text = html_text(content)
    if not all(label in text for label in ["Release ID: 2247870", "01 APR 2026", "Financial Year 2025-26"]):
        raise ValueError("Wrong PIB release or reporting period")
    patterns = {
        "constructed_length_km": r"constructed ([\d,]+) km of National Highways",
        "construction_target_km": r"target of ([\d,]+) km for the year",
        "capital_expenditure_inr_crore": r"was at Rs\. ([\d,]+) crore",
        "government_budgetary_support_inr_crore": r"Government Budgetary Support of Rs\. ([\d,]+) crore",
        "own_resources_funding_inr_crore": r"differential amount of Rs\. ([\d,]+) crore",
    }
    values = {}
    for metric, pattern in patterns.items():
        matches = re.findall(pattern, text)
        if not matches or len(set(matches)) != 1:
            raise ValueError(f"PIB labelled amount changed: {metric}")
        values[metric] = cell_number(matches[0])
    if values["capital_expenditure_inr_crore"] != values["government_budgetary_support_inr_crore"] + values["own_resources_funding_inr_crore"]:
        raise ValueError("Reported NHAI capital funding does not reconcile")
    return values


def _pin(builder: Any, sid: str) -> None:
    from pipelines.common import sha256_for_file
    path = document_path(builder.raw_root, sid)
    if sha256_for_file(path) != PINNED_SHA256[sid]:
        raise ValueError(f"Changed national publication requires reviewed re-extraction: {sid}")
    builder.pin(sid, path=path, url=DOCUMENTS[sid])
    builder.documents[sid][-1]["extractor"] = "pipelines.national_freshness; source-pinned reviewed table or labelled release extraction"
    if sid in REVIEWED_TABLE_FILES:
        table_path = builder.raw_root / "manual" / "evidence" / REVIEWED_TABLE_FILES[sid]
        builder.documents[sid][-1].update(reviewed_table_relative_path=str(table_path.relative_to(builder.raw_root)), reviewed_table_sha256=sha256_for_file(table_path))
        if sid.startswith("morth_"):
            listing = builder.raw_root / "manual/evidence/national_freshness_official_listing_records.json"
            builder.documents[sid][-1].update(listing_metadata_relative_path=str(listing.relative_to(builder.raw_root)), listing_metadata_sha256=sha256_for_file(listing))
    else:
        from pipelines.connectors.primary_disclosures import html_text
        builder.documents[sid][-1]["semantic_document_sha256"] = hashlib.sha256(html_text(path.read_text()).encode()).hexdigest()


def _safety(builder: Any) -> None:
    from pipelines.connectors.primary_disclosures import identifier
    sid = "morth_road_accidents_2024_final"
    tables = reviewed_tables(builder.raw_root, sid)
    validate_safety_tables(tables)
    for table in tables["tables"]:
        rows = [*table["rows"], {"state": "All India", "original_cells": table["published_total"]}]
        for row in rows:
            for year, cell in zip(table["years"], row["original_cells"]):
                state = row["state"]
                builder.fact(sid, table["metric"], cell_number(cell), "count", f"PDF p{table['page']}; printed p{table['printed_page']}; Annexure {table['annexure']}; {state}, {year} count column", entity_id="safety_" + identifier(state) + "_" + identifier(table["road_class"]), entity_name=state, entity_type="published_total" if state == "All India" else "state_ut", state=state, road_class=table["road_class"], start=f"{year}-01-01", end=f"{year}-12-31", basis="calendar_year", statement="MoRTH_road_accidents_statistical_final", reported_period=str(year), observation_status="final", assurance="official_statistical_final", notes="Final 2024 report vintage; comparative years retain this publication's revision identity. National Highways include Expressways. All-roads, NH and SH scopes are separate and not additive to each other. Printed count columns only; ranks/density/share columns excluded. All 36 state counts reconcile to the printed national total. Publication day undisclosed; portal featured date is discovery metadata.")
    builder.notes[sid] = "1,776 validated count facts:12 annexures,36 states/UTs plus national totals,4 calendar years 2021–24.48 state-column reconciliations pass. CY2024 cutoff is31 December 2024; no 2026 safety actual inferred. Prior report vintages retained elsewhere."


def _performance(builder: Any) -> None:
    sid = "nhai_fy2025_26_performance"
    values = extract_performance(document_path(builder.raw_root, sid).read_text())
    for metric, value in values.items():
        target = metric == "construction_target_km"
        builder.fact(sid, metric, value, "km" if metric.endswith("_km") else "INR crore", "PIB PRID2247870; construction paragraph" if metric.endswith("_km") else "PIB PRID2247870; capital expenditure and funding paragraph", agency="NHAI", start="2025-04-01", end="2026-03-31", statement="NHAI_administrative_performance", asof="2026-03-31", published="2026-04-01", disclosure_as_of="2026-04-01", reported_period="2025-26", estimate="target" if target else "actual", evidence="target" if target else "official_measured", eligible=not target, observation_status="estimate" if target else "reported", assurance="administrative_reported", implementing_agency_id="nhai", notes="FY2025–26 complete-year administrative release, not audited financial statements. Own resources meet the disclosed capex/support differential and are not gross toll collections. Target is disclosed separately from achievement.")
    builder.notes[sid] = "Official 1 April 2026 release:5,313 km constructed; capex ₹244,362 crore, Government Budgetary Support ₹238,384 crore and own resources ₹5,978 crore reconcile.4,640 km target remains ineligible for measured calculations."


def _annual(builder: Any) -> None:
    from pipelines.connectors.primary_disclosures import fiscal_period, identifier
    sid = "morth_annual_report_2025_26"
    tables = reviewed_tables(builder.raw_root, sid)
    common = dict(published=ANNUAL_PUBLICATION, assurance="administrative_reported", observation_status="reported")
    lengths = sum(cell_number(r["length_km"]) for r in tables["network"])
    if len(tables["network"]) != 36 or lengths != 146570 or tables["network_published_total_km"] != 146572:
        raise ValueError("Reviewed network table or documented2km discrepancy changed")
    for row in tables["network"]:
        for metric, cell, unit in [("nh_network_length_km", row["length_km"], "km"), ("nh_number_count", row["nh_count"], "count")]:
            builder.fact(sid, metric, cell_number(cell), unit, f"PDF p{row['page']}; printed p{row['page']-2}; Appendix2, {row['state']}", entity_id="network_" + identifier(row["state"]), entity_name=row["state"], entity_type="state_ut", state=row["state"], end="2025-12-31", basis="stock", statement="MoRTH_Appendix2_NH_network", reported_period="as on31December2025", notes="State/UT reported NH network; separate Dadra and Nagar Haveli and Daman and Diu labels preserved as published; Lakshadweep absent. Source state lengths sum 146,570 km versus published 146,572 km; no balancing adjustment. State NH number counts overlap across states and must not be summed into a national unique-route count.", **common)
    for metric, value, unit in [("nh_network_length_km",146572,"km"),("nh_number_count",670,"count")]:
        builder.fact(sid, metric, value, unit, "PDF p130; printed p128; Appendix2 published Total Length row", entity_id="morth_network_published_total", entity_name="Published national NH network total", entity_type="published_total", end="2025-12-31", basis="stock", statement="MoRTH_Appendix2_NH_network", eligible=False, notes="Published length 146,572 km exceeds state sum 146,570 km by2km. National count 670 is not the sum of state NH counts; unique-route interpretation is not inferred. Both reported aggregate cells remain readable but excluded from arithmetic.", **common)
    if len(tables["crif"]) != 26:
        raise ValueError("CRIF historical coverage changed")
    for row in tables["crif"]:
        year = int(row["fiscal_year"][:4]); start, full_end = fiscal_period(year)
        for label in ["allocation", "release"]:
            actual = label == "release"
            end = "2025-12-31" if actual and year == 2025 else full_end
            builder.fact(sid, f"crif_state_roads_{label}_inr_crore", cell_number(row[label]), "INR crore", f"PDF p131; printed p129; Appendix3 FY{row['fiscal_year']} {label}", entity_id="crif_state_roads", entity_name="CRIF State Roads", road_class="State roads (multiple classes)", start=start, end=end, estimate="YTD" if actual and year == 2025 else "actual" if actual else "allocation", asof=end if actual else "", eligible=actual, evidence="official_measured" if actual else "target", observation_status="reported" if actual else "estimate", statement="MoRTH_Appendix3_CRIF", reported_period=row["fiscal_year"], notes="Release is an administrative cash funding flow, not expenditure or physical achievement. Current release footnote explicitly till 31 December 2025. Allocation is a disclosed funding plan; original approval/vintage date undisclosed, excluded from arithmetic. State roads is broader than SH only.", published=ANNUAL_PUBLICATION, assurance="administrative_reported")
    permit_total = sum(cell_number(r["original_INR"]) for r in tables["permit"])
    if len(tables["permit"]) != 33 or permit_total != tables["permit_published_total_INR"]:
        raise ValueError("National permit-fee disbursement reconciliation failed")
    for row in [*tables["permit"], {"state":"All India","original_INR":tables["permit_published_total_INR"]}]:
        state = row["state"]
        builder.fact(sid, "national_permit_fee_disbursement_inr_crore", cell_number(row["original_INR"]), "INR crore", f"PDF p133; printed p131; Appendix5 {state}", original_unit="INR", entity_id="permit_" + identifier(state), entity_name=state, entity_type="published_total" if state=="All India" else "state_ut", state=state, road_class="All road transport", start="2025-04-01", end="2025-12-31", estimate="YTD", statement="MoRTH_Appendix5_National_Permit_fee", notes="Original Rs. in Actual means absolute INR, multiplied by 0.0000001 to crore. State-wise National Permit fee disbursement; neither gross toll collections nor highway expenditure.33 state cells reconcile to INR18,723,045,000.", **common)
    if len(tables["major_heads"]) != 22:
        raise ValueError("Major-head table coverage changed")
    for row in tables["major_heads"]:
        for column in ["BE", "YTD"]:
            actual = column == "YTD"
            builder.fact(sid, "major_head_expenditure_inr_crore", cell_number(row[column]), "INR crore", f"PDF p134; printed p132; Appendix6 {row['label']}, {column}", entity_id="morth_" + row["id"], entity_name="MoRTH " + row["label"], road_class="All road transport" if row["id"] in {"mh3055","mh5055","mh3451","mh5475"} else "All roads (ministry accounts)", start="2025-04-01", end="2025-12-31" if actual else "2026-03-31", asof="2025-12-31" if actual else "", estimate="YTD" if actual else "BE", eligible=actual, evidence="official_measured" if actual else "target", observation_status="reported" if actual else "estimate", statement="MoRTH_Appendix6_major_head_cash_accounts", reported_period="FY2025-26 through December 2025" if actual else "FY2025-26 BE", notes="Gross, recoveries, voted/charged and net rows are separate accounting views and must not be added. Negative recoveries retained. Explicit zero cells retained; NE expenditure shown through functional 5054 head. Detailed net capital YTD ₹228,952.36 crore differs from narrative overview ₹227,021 crore; no substitution. BE original vintage undisclosed in this report, so cutoff/vintage blank and arithmetic disabled.", published=ANNUAL_PUBLICATION, assurance="administrative_reported")
    for row in tables["operational"]:
        builder.fact(sid, row["metric"], cell_number(row["value"]), "km", f"PDF p{row['page']}; printed p{row['page']-2}; Section2.3 Award and Construction", start=row["start"], end=row["end"], estimate="YTD" if row["estimate"]=="YTD_actual" else "actual", statement="MoRTH_all_NH_administrative_performance", notes="MoRTH-wide National Highway scope is distinct from NHAI-only 5,313 km FY2025-26. Current rows explicitly through December 2025, not complete FY2025–26.", **common)
    builder.notes[sid] = "207 reviewed facts:36 state network rows×2plus 2 quarantined national cells;26 CRIF years×2;33 state permit disbursements plus total;22 major-head rows×BE/YTD;3 construction/award observations. Observation dates per table, publication 30 March 2026 separate.50 funding estimates/aggregate cells excluded from calculations; no full FY2025–26 cash actuals inferred."


def _financial(builder: Any) -> None:
    sid = "nhai_financial_results_2025_03_unaudited"
    tables = reviewed_tables(builder.raw_root, sid)
    for family, page in [("balance_sheet",2),("cash_flow",4)]:
        for row in tables[family]:
            for year, cell in zip(tables["years"], row["original_cells"]):
                value = cell_number(cell)
                if value is None:
                    continue
                end = f"{year}-03-31"
                builder.fact(sid, row["metric"], value, "INR crore", f"PDF p{page}; printed p{page-1}; {row['label']};31March{year} column", original_unit="INR lakh", agency="NHAI", start=f"{year-1}-04-01" if family=="cash_flow" else "", end=end, basis="fiscal_year" if family=="cash_flow" else "balance_sheet_snapshot", statement="NHAI_standalone_cash_flow" if family=="cash_flow" else "NHAI_standalone_balance_sheet", reported_period=f"FY{year-1}-{str(year)[2:]}" if family=="cash_flow" else end, assurance="limited_review_unaudited" if year==2025 else "audited_comparator", implementing_agency_id="nhai", financing_entity_id="nhai", notes="Current 31 March 2025 column unaudited and subject to limited review; prior 31 March 2024 comparative labelled audited. Original Rs. in Lakhs×0.01. Authority implements Government road assets; conventional corporate profit/interest ratios can mislead. Bond-interest cash payment is distinct from establishment finance cost. Cash adjusted toll ploughback is not gross toll receipts. Reported capital-road-work total includes net fixed block; not only completed+WIP. Publication day unverified and blank; comparative cutoff never advanced to newer report year.")
    for row in tables["debt"]:
        builder.fact(sid,"debt_total_inr_crore",cell_number(row["original_value"]),"INR crore",f"PDF p6; printed p5; Working row13 TOTAL DEBT, {row['end']} column",original_unit="INR lakh",agency="NHAI",end=row["end"],basis="balance_sheet_snapshot",statement="NHAI_standalone_debt_working",reported_period=row["end"],assurance=row["assurance"],implementing_agency_id="nhai",financing_entity_id="nhai",notes="Reported total debt, not a sum of working-sheet assets/liabilities.31 March 2025 unaudited/limited-review;31 December 2024 unaudited comparator;31 March 2024 audited comparator. Lease/short-term/current-maturity dash cells remain missing, not zeros; no maturity schedule inferred. Original INR lakh×0.01; publication day unknown.")
    # Check meaningful statement relationships in original lakh units.
    balance = {r["metric"]: r["original_cells"] for r in tables["balance_sheet"]}
    for i, debt in [(0,24460406.54),(1,33537319.66)]:
        if not math.isclose(cell_number(balance["secured_borrowings_inr_crore"][i]) + cell_number(balance["unsecured_borrowings_inr_crore"][i]), debt, abs_tol=.01):
            raise ValueError("Borrowings do not reconcile to disclosed total debt")
    builder.notes[sid] = "74 validated financial facts:38 balance-sheet cells,33 annual cash-flow cells (one current dash omitted),3 dated debt balances. Current FY2024–25 limited-review/unaudited; FY2023–24 comparator labelled audited. No balance cutoff derived from publication, no dash converted to zero, and no later quarter assumed available."


def extend_snapshots(builder: Any) -> None:
    """Add exact national publication vintages to the shared governed builder."""
    functions = {SOURCE_IDS[0]:_safety, SOURCE_IDS[1]:_performance, SOURCE_IDS[2]:_annual, SOURCE_IDS[3]:_financial}
    for sid in SOURCE_IDS:
        builder.rows.setdefault(sid, [])
        builder.documents.setdefault(sid, [])
        if not document_path(builder.raw_root, sid).exists():
            builder.notes[sid] = "Pinned national publication absent from this raw cache; no numeric extraction performed. Prior validated extract, if present, must be retained."
            continue
        _pin(builder, sid)
        functions[sid](builder)


if __name__ == "__main__":
    from pipelines.connectors.primary_disclosures import SnapshotBuilder
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-root", type=Path, default=Path("data/raw"))
    parser.add_argument("--cutoff", default="2026-10-02")
    args = parser.parse_args()
    builder = SnapshotBuilder(args.raw_root, cutoff=args.cutoff)
    extend_snapshots(builder)
    builder.finish(SOURCE_IDS)
