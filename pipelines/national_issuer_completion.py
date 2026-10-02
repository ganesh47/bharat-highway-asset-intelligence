"""Reviewed NHAI financial vintages and dated UPEIDA project progress.

Publisher PDFs are checksum-bound to rendered, reviewed cells. Filing order is
supported by reporting periods and explicit recast notes, never an invented
publication date. Each filing's typed observations remain in immutable history.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import shutil
from pathlib import Path
from typing import Any

import pandas as pd

NHAI_SOURCE = "nhai_financial_results_2025_26_to_2026_06"
UPEIDA_SOURCE = "upeida_ganga_progress_2026_04_27"
DOCUMENTS = {
    NHAI_SOURCE: "https://nhai.gov.in/nhai/sites/default/files/mix_file/Quarterly_Financials_for_the_Quarter_ended_on_30th_June_2026.pdf",
    UPEIDA_SOURCE: "https://upeida.up.gov.in/downloadmedia/siteContent/202604271802004244Ganga%20Pdf.pdf",
}
SOURCE_IDS = tuple(DOCUMENTS)
HTML_SOURCE_IDS: tuple[str, ...] = ()
HTML_RECHECK_SOURCE_IDS: tuple[str, ...] = ()
REVIEWED_FILES = {
    NHAI_SOURCE: NHAI_SOURCE + "_reviewed_tables.json",
    UPEIDA_SOURCE: UPEIDA_SOURCE + "_reviewed_table.json",
}
REVIEWED_SHA256 = {
    NHAI_SOURCE: "bcf15368774b2e4878cae134b5f624e595525c39dd23f69e3c94fcab595d712e",
    UPEIDA_SOURCE: "811d8d61ad143bd6a4af303664f2f63b74d610b046ac64eb55511392e2e24e89",
}
NHAI_HASHES = (
    "ceec789c08d5a5e81cac05776bbd82030c1f9d9765798fc7133080387efdfe85",
    "175094a77bd0bfe7b9022afd00b0c9959692bb10a55e571949276b141a765f07",
    "84d6384e7ab6c246d80df7732ad640053c999a9339d16757f64a91185506a443",
    "f5ec8c532d75ee286d62ec1663e25258f11eac3e6023b27094818081e81b9804",
    "d3eb43cde399c827cfc0d7f12384559ec76120450bef526cfc75f3c1d7b799d0",
)
UPEIDA_HASH = "b387e33686b57676bdf259f5e18ba239a17789cd33f3b968e89a0ae7adc9a399"


def document_path(raw_root: Path, sid: str) -> Path:
    return raw_root / "national_issuer_completion" / sid / ("quarter_0.pdf" if sid == NHAI_SOURCE else "document.pdf")


def reviewed_tables(raw_root: Path, sid: str) -> dict[str, Any]:
    path = raw_root / "manual" / "evidence" / REVIEWED_FILES[sid]
    if hashlib.sha256(path.read_bytes()).hexdigest() != REVIEWED_SHA256[sid]:
        raise ValueError("Reviewed table checksum changed; a new visual review is required")
    table = json.loads(path.read_text())
    if table.get("source_id") != sid or table.get("reviewed_at") != "2026-10-02" or not table.get("review_method"):
        raise ValueError("Missing source-specific visual-review lineage")
    if sid == NHAI_SOURCE and tuple(doc["sha256"] for doc in table["documents"]) != NHAI_HASHES:
        raise ValueError("Reviewed filing provenance does not match five verified NHAI PDFs")
    if sid == UPEIDA_SOURCE and table.get("document_sha256") != UPEIDA_HASH:
        raise ValueError("Reviewed project provenance does not match the verified UPEIDA PDF")
    return table


def cell_number(cell: Any) -> float | None:
    text = str(cell).strip()
    if text in {"", "-", "–", "—", "NA", "N/A", "..."}:
        return None
    if not re.fullmatch(r"\(?-?\d[\d,]*(?:\.\d+)?\)?", text):
        raise ValueError("Unreviewed numeric cell: " + text)
    value = float(text.strip("()").replace(",", ""))
    return -value if text.startswith("(") else value


def _near(left: float, right: float, label: str) -> None:
    # Original statements round cells to two decimals of INR lakh.
    if not math.isclose(left, right, abs_tol=.03, rel_tol=0):
        raise ValueError("Financial statement reconciliation failed: " + label)


def validate_nhai_tables(tables: dict[str, Any]) -> None:
    periods = [doc["quarter_end"] for doc in tables["documents"]]
    if periods != ["2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31", "2026-06-30"]:
        raise ValueError("Quarter sequence changed")
    for doc in tables["documents"]:
        if doc["published_at"]:
            raise ValueError("Publication day requires independent publisher evidence")
        for column in doc["flow"]["columns"]:
            if len(column["cells"]) != len(tables["flow_metrics"]):
                raise ValueError("Missing flow cell cannot shift financial columns")
            value = [cell_number(cell) for cell in column["cells"]]
            _near(value[0] + (value[1] or 0), value[2], "income")
            _near(sum(value[3:8]), value[8], "expenditure")
            _near(value[2] - value[8], value[9], "income less expenditure")
            _near(value[9] + value[10], value[11], "net deficit")
        if "balance_sheet" in doc:
            balance = doc["balance_sheet"]
            if len(balance["cells"]) != len(tables["balance_sheet_metrics"]):
                raise ValueError("Incomplete balance-sheet columns")
            value = [cell_number(cell) for cell in balance["cells"]]
            _near(sum(value[:4]), value[4], "sources")
            _near(value[5] - value[6], value[7], "net fixed assets")
            _near(sum(value[7:10]), value[10], "capital road work including fixed block")
            _near(value[12] + value[13], value[14], "current assets")
            _near(value[15] + value[16], value[17], "current liabilities and provisions")
            _near(value[14] - value[17], value[18], "net current assets")
            _near(value[10] + value[11] + value[18] + (value[19] or 0), value[20], "applications")
            _near(value[4], value[20], "sources and applications")
        if "cash_flow" in doc:
            value = [cell_number(cell) for cell in doc["cash_flow"]["cells"]]
            if len(value) != len(tables["cash_flow_metrics"]) or any(v is None for v in value):
                raise ValueError("Incomplete selected cash-flow cells")
            _near(value[0] + value[2] + value[13], value[14], "net cash movement")
            _near(value[15] + value[14], value[16], "opening to closing cash")
        for column in doc["working"]["columns"]:
            if len(column["cells"]) != len(tables["working_metrics"]):
                raise ValueError("Incomplete working stock cells")
            debt, _, _, _, _, secured = map(cell_number, column["cells"])
            if secured > debt:
                raise ValueError("Secured borrowing exceeds disclosed total debt")


def nhai_vintages(builder: Any, tables: dict[str, Any]) -> list[pd.DataFrame]:
    """Extract selected cells from each original filing, without revision merging."""
    from pipelines.connectors.primary_disclosures import FACT_COLUMNS, validate_facts
    validate_nhai_tables(tables)
    vintages = []
    for index, doc in enumerate(tables["documents"]):
        builder.rows[NHAI_SOURCE] = []

        def emit(metric, cell, page, *, start="", end="", basis="balance_sheet_snapshot", statement="", status="reported", notes=""):
            value = cell_number(cell)
            if value is None:
                return  # A printed dash is not a zero financial fact.
            builder.fact(NHAI_SOURCE, metric, value, "INR crore", f"PDF p{page}; {statement}; {metric}; {end} column", original_unit="INR lakh", agency="NHAI", entity_id="nhai", entity_name="NHAI", entity_type="agency", start=start, end=end, basis="fiscal_quarter" if basis == "quarter" else basis, statement=statement, published="", disclosure_as_of=doc["quarter_end"], reported_period=end if not start else start + " to " + end, observation_status=status, assurance="limited_review_unaudited", revision_identity=doc["sha256"], implementing_agency_id="nhai", financing_entity_id="nhai", document_index=index, notes="Original INR lakh×0.01; unaudited limited-review authority filing, not CAG-audited. Publication day unknown. Financial periods retain their own observation cutoff, including reprinted comparatives. " + notes)

        for column in doc["flow"]["columns"]:
            for metric, cell in zip(tables["flow_metrics"], column["cells"]):
                emit(metric, cell, doc["flow"]["page"], start=column["start"], end=column["end"], basis=column["basis"], status=column["status"], statement="NHAI_standalone_income_expenditure", notes="NHAI acts as implementing agency for Government assets; deficit is not a commercial operating margin. Quarter and fiscal YTD/annual are separate scopes. June2026 notes(g),(i) explicitly recast March2026 quarter/year comparatives.")
        if "balance_sheet" in doc:
            item = doc["balance_sheet"]
            for metric, cell in zip(tables["balance_sheet_metrics"], item["cells"]):
                emit(metric, cell, item["page"], end=item["end"], statement="NHAI_standalone_balance_sheet", notes="Classic sources/applications statement; reported capital-road-work includes net fixed block. This original balance sheet is not silently restated using the separate SEBI Working table.")
        if "cash_flow" in doc:
            item = doc["cash_flow"]
            for metric, cell in zip(tables["cash_flow_metrics"], item["cells"]):
                emit(metric, cell, item["page"], start=item["start"], end=item["end"], basis=item["basis"], statement="NHAI_standalone_cash_flow", notes="Cash-flow bond interest differs from establishment finance charges. Adjusted toll ploughback cash is neither gross toll collection nor gross CFI deposit. Negative repayments retain their printed sign.")
        for column in doc["working"]["columns"]:
            for metric, cell in zip(tables["working_metrics"], column["cells"]):
                emit(metric, cell, doc["working"]["page"], end=column["end"], status=column["status"], statement="NHAI_standalone_debt_working", notes="SEBI Working stock table; no sums of missing lease/short-term borrowing cells. September2025 uses the same-end-date YTD stock column consistent with its balance sheet. June2026 March2026 comparatives explicitly recast; original filing remains archived.")
        for row in doc["note_facts"]["facts"]:
            statement = "NHAI_gross_toll_collections" if row["metric"] == "toll_collection_inr_crore" else "NHAI_toll_deposits_CFI" if row["metric"] == "toll_deposit_cfi_inr_crore" else "NHAI_contingent_claims"
            emit(row["metric"], row["cell"], doc["note_facts"]["page"], start=row["start"], end=row["end"], basis=row["basis"], status=row["status"], statement=statement, notes="Gross collections, CFI deposits, cash ploughback and contingent claims are separate metrics, not additive revenue. Claims in June2026 filing refer to31March2026; June-quarter claims were not compiled. Clipped December CFI deposit and March ambiguous comparative quarter label are omitted. September notes(g) explicitly recast June2025 comparative toll/claim figures.")
        frame = pd.DataFrame(builder.rows[NHAI_SOURCE], columns=FACT_COLUMNS)
        vintages.append(validate_facts(frame, NHAI_SOURCE, {"documents": builder.documents[NHAI_SOURCE]}, builder.research_cutoff))
    return vintages


def extract_nhai(builder: Any, tables: dict[str, Any], *, persist_history: bool = True) -> pd.DataFrame:
    from pipelines.publication_history import merge_observations, record_versions
    from pipelines.connectors.primary_disclosures import FACT_COLUMNS
    canonical = pd.DataFrame(columns=FACT_COLUMNS)
    for index, frame in enumerate(nhai_vintages(builder, tables)):
        canonical, revisions = merge_observations(canonical, frame)
        if persist_history:
            doc = dict(builder.documents[NHAI_SOURCE][index])
            original = builder.raw_root / doc["relative_path"]
            archive = builder.raw_root / "primary_disclosures" / NHAI_SOURCE / "versions" / (doc["sha256"] + ".pdf")
            archive.parent.mkdir(parents=True, exist_ok=True)
            if not archive.exists():
                shutil.copyfile(original, archive)
            if hashlib.sha256(archive.read_bytes()).hexdigest() != doc["sha256"]:
                raise ValueError("Archived filing bytes changed")
            doc["relative_path"] = str(archive.relative_to(builder.raw_root))
            # Archive the typed original filing, not only the merged latest view.
            evidence = {"source_id": NHAI_SOURCE, "documents": [doc], "extraction_status": "validated", "research_cutoff": builder.research_cutoff, "reviewed_table_sha256": REVIEWED_SHA256[NHAI_SOURCE], "filing_period_end": tables["documents"][index]["quarter_end"], "published_at": None, "assurance": "limited_review_unaudited", "row_count": len(frame)}
            record_versions(builder.raw_root / "manual" / "history", NHAI_SOURCE, None, (frame, evidence), revisions)
    builder.rows[NHAI_SOURCE] = canonical.to_dict("records")
    builder.notes[NHAI_SOURCE] = tables["scope_notes"]
    return canonical


def extract_upeida(builder: Any, table: dict[str, Any]) -> None:
    if table["data_as_of"] != "2026-04-27" or table["published_at"] != "2026-04-27":
        raise ValueError("Progress cutoff/publication must be supported independently")
    support = table["publication_support"]
    index = builder.raw_root / support["snapshot_path"]
    if hashlib.sha256(index.read_bytes()).hexdigest() != support["sha256"]:
        raise ValueError("Official index publication-date evidence changed")
    if not re.search(r"<td>27-04-2026</td>\s*<td>.*?202604271802004244Ganga Pdf\.pdf", index.read_text(), re.S):
        raise ValueError("Exact dated publisher progress link is absent")
    facts = table["facts"]
    if len(facts) != 8 or len({row["metric"] for row in facts}) != 8:
        raise ValueError("Incomplete dated Ganga progress table")
    counts = {row["metric"]: cell_number(row["cell"]) for row in facts}
    if counts["completed_structures_count"] > counts["total_structures_count"]:
        raise ValueError("Completed structures exceed disclosed total")
    for row in facts:
        value = cell_number(row["cell"])
        if value is None or (row["unit"] == "percent" and not 0 <= value <= 100):
            raise ValueError("Invalid verified progress cell")
        builder.fact(UPEIDA_SOURCE, row["metric"], value, row["unit"], "PDF p1; Ganga construction progress table; printed date27.04.2026; " + row["label"], entity_id="upeida_ganga_expressway", entity_name="Ganga Expressway", entity_type="project", agency="UPEIDA", state="Uttar Pradesh", road_class="Expressway", end=table["data_as_of"], basis="project_snapshot", statement="UPEIDA_Ganga_construction_progress", published=table["published_at"], disclosure_as_of=table["data_as_of"], reported_period="as on27April2026", assurance="administrative_reported", observation_status="reported", implementing_agency_id="upeida", project_name="Ganga Expressway", notes="Printed PDF date27.04.2026 is the observation cutoff; independent official link-table upload date27Apr2026 supports publication. Native Hindi-font extraction drops the date, so it was verified on the rendered original. Physical progress snapshot, not fiscal spending, NH-wide scope or post-inauguration current progress. Component100% is distinct from overall98%; structure counts are not lengths. The progress document identifies UPEIDA as reporting/implementing authority; asset ownership, operator and concessionaire are not established and remain unspecified.")
    builder.notes[UPEIDA_SOURCE] = "Eight dated administrative progress facts, verified rendered one-page publisher PDF. Observed/published27April2026; the later inauguration29April2026 does not advance these facts. No financial revenue or current October progress inferred. Asset owner/operator/concessionaire are not established by this progress disclosure."


def extend_snapshots(builder: Any) -> None:
    for sid in SOURCE_IDS:
        builder.rows.setdefault(sid, [])
        builder.documents.setdefault(sid, [])
        table = reviewed_tables(builder.raw_root, sid) if (builder.raw_root / "manual" / "evidence" / REVIEWED_FILES[sid]).exists() else None
        documents = table["documents"] if sid == NHAI_SOURCE and table else [{"file": "document.pdf", "url": DOCUMENTS[sid], "sha256": UPEIDA_HASH}] if sid == UPEIDA_SOURCE else []
        paths = [builder.raw_root / "national_issuer_completion" / sid / doc["file"] for doc in documents]
        if not table or not paths or not all(path.exists() for path in paths):
            builder.notes[sid] = "Reviewed publisher PDF cache is incomplete; no new numeric extraction. Previously validated CSV/evidence must be retained."
            continue
        builder.rows[sid], builder.documents[sid] = [], []
        for doc, path in zip(documents, paths):
            if hashlib.sha256(path.read_bytes()).hexdigest() != doc["sha256"]:
                raise ValueError("Publisher document changed; reviewed cells cannot bind to new bytes")
            builder.pin(sid, path, doc["url"])
        if sid == NHAI_SOURCE:
            extract_nhai(builder, table)
        else:
            extract_upeida(builder, table)


if __name__ == "__main__":
    from pipelines.connectors.primary_disclosures import SnapshotBuilder
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-root", type=Path, default=Path("data/raw"))
    parser.add_argument("--cutoff", default=None)
    args = parser.parse_args()
    builder = SnapshotBuilder(args.raw_root, cutoff=args.cutoff)
    extend_snapshots(builder)
    builder.finish(SOURCE_IDS)
    print(json.dumps({sid: len(builder.rows[sid]) for sid in SOURCE_IDS}))
