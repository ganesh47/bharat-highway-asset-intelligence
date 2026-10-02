"""Reviewed FY2024-25 Roads/Bridges flows from 21 additional CAG accounts.

Finance Accounts contain annual and cumulative columns, rotated tables, scans,
and occasional published rounding differences. The committed reviewed-cell
bundle binds labelled cells to the original PDF bytes; a changed publication
requires a new review rather than automatic admission of an OCR candidate.
"""
from __future__ import annotations

import hashlib
import json
import re
import tempfile
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = ROOT / "data/raw/manual/evidence/cag_state_accounts_completion_reviewed_tables.json"
REVIEW_SHA256 = "0767b8c698dacbb1228467e910295baa6b7330c190c0225c47a601607dbccd1d"
MAX_DOCUMENT_BYTES = 100_000_000
FUNCTION_COLUMNS = ["revenue", "capital", "loans_and_advances", "total"]
CAPITAL_COLUMNS = ["expenditure_2023_24", "progressive_2023_24", "expenditure_2024_25", "progressive_2024_25"]


def reviewed_tables(path: Path = REVIEW_PATH) -> dict[str, Any]:
    """Verify review provenance before any candidate amounts are read."""
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("CAG reviewed-table checksum changed; reviewed re-extraction required")
    payload = json.loads(raw)
    if payload.get("schema_version") != 1 or payload.get("research_cutoff") != "2026-10-02":
        raise ValueError("CAG review edition contract changed")
    return payload["documents"]


TABLES = reviewed_tables()
DOCUMENTS = {sid: record["url"] for sid, record in TABLES.items()}
SOURCE_IDS = tuple(DOCUMENTS)
PINNED_SHA256 = {sid: record["sha256"] for sid, record in TABLES.items()}


def document_path(raw_root: Path, sid: str) -> Path:
    return raw_root / "state_accounts_completion" / sid / "document.pdf"


def cell_number(cell: str) -> Decimal | None:
    """Missing marks stay missing; only an explicit numeric zero becomes zero."""
    text = cell.strip()
    if text.lower() in {"", ".", "..", "...", "…", "-", "—", "–", "nil", "n/a", "na"}:
        return None
    if not re.fullmatch(r"-?\d+(?:,\d+)*\.\d{2}", text):
        raise ValueError(f"Ambiguous reviewed amount: {cell!r}")
    return Decimal(text.replace(",", ""))


def validate_table(record: dict[str, Any]) -> dict[str, Any]:
    """Select flows by labels and reconcile them without correcting the source."""
    if record["unit"] != "INR crore" or record["reporting_period"] != "2024-25":
        raise ValueError("CAG unit or reporting-period contract changed")
    if not record.get("visual_review_complete") or record.get("publication_date") is not None:
        raise ValueError("Incomplete review or invented publication date")
    function, capital = record["function"], record["capital"]
    if function["columns"] != FUNCTION_COLUMNS or capital["major_head"] != "5054":
        raise ValueError("CAG statement/head/column contract changed")
    expected = CAPITAL_COLUMNS[:]
    if record["state"] == "Uttarakhand":
        expected = expected[2:] + expected[:2]
    if record["state"] == "Andhra Pradesh":
        expected.insert(2, "allocated_amount")
    if capital["columns"] != expected or set(capital["cells"]) != set(expected):
        raise ValueError("CAG annual/cumulative column contract changed")
    if set(function["cells"]) != set(FUNCTION_COLUMNS):
        raise ValueError("CAG functional cells missing; no shifted amount columns")
    numbers = {key: cell_number(value) for key, value in function["cells"].items()}
    flows = {key: cell_number(value) for key, value in capital["cells"].items()}
    if any(numbers[key] is None for key in ("revenue", "capital", "total")):
        raise ValueError("CAG required actual cell missing; cannot fill with zero")
    if any(flows[key] is None for key in CAPITAL_COLUMNS):
        raise ValueError("CAG annual/cumulative amount missing; review required")
    # Sum only disclosed numbers. A blank loan cell is not turned into a fact
    # or a zero. Its absence is preserved in the governed reviewed-cell bundle.
    known_components = [numbers[key] for key in FUNCTION_COLUMNS[:-1] if numbers[key] is not None]
    if abs(sum(known_components) - numbers["total"]) > Decimal("0.005"):
        raise ValueError("CAG functional row reconciliation failed")
    difference = flows["expenditure_2024_25"] - numbers["capital"]
    discrepancy = bool(record.get("cross_table_discrepancy"))
    if difference:
        if not discrepancy or abs(difference) != Decimal("0.01"):
            raise ValueError("Unexpected CAG cross-table discrepancy; no synthetic balancing")
    elif discrepancy:
        raise ValueError("CAG discrepancy flag no longer matches the disclosed amounts")
    return {
        "revenue_2024_25": numbers["revenue"],
        "capital_2024_25": flows["expenditure_2024_25"],
        "capital_2023_24": flows["expenditure_2023_24"],
        "functional_capital_2024_25": numbers["capital"],
        "cross_table_difference": difference,
        "current_capital_eligible": not discrepancy,
        "published_loan_cell": function["cells"]["loans_and_advances"],
    }


def extract_account(path: Path, sid: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """Extract only the reviewed cells from their pinned primary publication."""
    from pypdf import PdfReader

    record = reviewed_tables()[sid]
    if hashlib.sha256(path.read_bytes()).hexdigest() != record["sha256"]:
        raise ValueError(f"{sid}: changed primary PDF requires reviewed re-extraction")
    reader = PdfReader(path)
    if len(reader.pages) != record["pages"]:
        raise ValueError("CAG physical page-count contract changed")
    for table in (record["function"], record["capital"]):
        if not 1 <= table["physical_page"] <= len(reader.pages):
            raise ValueError("CAG reviewed physical page outside document")
    return validate_table(record), record


def _download_one(raw_root: Path, sid: str) -> dict[str, Any]:
    """Bounded TLS-verified download; changed bytes never replace pinned inputs."""
    url, path = DOCUMENTS[sid], document_path(raw_root, sid)
    parsed = urlsplit(url)
    if parsed.scheme != "https" or parsed.hostname != "cag.gov.in":
        raise ValueError("CAG download requires the verified primary HTTPS host")
    if path.exists() and hashlib.sha256(path.read_bytes()).hexdigest() == PINNED_SHA256[sid]:
        return {"source_id": sid, "status": "pinned_cache_available"}
    path.parent.mkdir(parents=True, exist_ok=True)
    request = urllib.request.Request(url, headers={"User-Agent": "BHAI-state-accounts-completion/1.0"})
    with urllib.request.urlopen(request, timeout=90) as response:
        resolved = urlsplit(response.url)
        if resolved.scheme != "https" or resolved.hostname != "cag.gov.in":
            raise ValueError("Unverified CAG redirect")
        with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as staged:
            staged_path = Path(staged.name)
            size = 0
            try:
                while content := response.read(1_048_576):
                    size += len(content)
                    if size > MAX_DOCUMENT_BYTES:
                        raise ValueError("CAG document exceeds bounded download size")
                    staged.write(content)
            except BaseException:
                staged_path.unlink(missing_ok=True)
                raise
    try:
        content = staged_path.read_bytes()
        if not content.startswith(b"%PDF-") or hashlib.sha256(content).hexdigest() != PINNED_SHA256[sid]:
            raise ValueError("CAG document changed or is not PDF; reviewed re-extraction required")
        staged_path.replace(path)
    finally:
        staged_path.unlink(missing_ok=True)
    return {"source_id": sid, "status": "pinned_document_retrieved", "sha256": PINNED_SHA256[sid], "size_bytes": size}


def download_documents(raw_root: Path, source_ids: tuple[str, ...] = SOURCE_IDS) -> list[dict[str, Any]]:
    with ThreadPoolExecutor(max_workers=4) as pool:
        return list(pool.map(lambda sid: _download_one(raw_root, sid), source_ids))


def extend_snapshots(builder: Any) -> None:
    """Core integration hook; partial caches preserve explicit evidence gaps."""
    for sid in SOURCE_IDS:
        builder.rows.setdefault(sid, [])
        builder.documents.setdefault(sid, [])
        path = document_path(builder.raw_root, sid)
        if not path.exists():
            builder.notes[sid] = "Available CAG publication identified, pinned document unavailable locally; no unverified numerical facts admitted."
            continue
        values, record = extract_account(path, sid)
        if not builder.documents[sid]:
            builder.pin(sid, path, DOCUMENTS[sid])
        builder.documents[sid][-1].update(
            extractor="pipelines.state_accounts_completion",
            reviewed_tables_relative_path=str(REVIEW_PATH.relative_to(ROOT)),
            reviewed_tables_sha256=REVIEW_SHA256,
            review_date="2026-10-02",
            source_catalogue_url=record["index"],
        )
        state = record["state"]
        definitions = [
            ("roads_bridges_revenue_expenditure_inr_crore", "revenue_2024_25", 2024, "function", "Revenue column, functional Roads and Bridges", True),
            ("roads_bridges_capital_outlay_inr_crore", "capital_2024_25", 2024, "capital", "5054, expenditure during2024-25 column", values["current_capital_eligible"]),
            ("roads_bridges_capital_outlay_inr_crore", "capital_2023_24", 2023, "capital", "5054, expenditure during2023-24 comparative column", True),
        ]
        if not values["current_capital_eligible"]:
            definitions.append(("roads_bridges_capital_outlay_functional_inr_crore", "functional_capital_2024_25", 2024, "function", "Capital column, alternative published functional Roads and Bridges cell", False))
        for metric, key, year, table_key, column, eligible in definitions:
            table = record[table_key]
            notes = "Audited state-government cash accounts as reported; functional Roads and Bridges covers multiple road classes. Not State Highway-only spending, corridor cost, or corporation finances. Annual flows selected by labelled columns, excluding progressive expenditure and head5055 Road Transport. Publication/signature/retrieval dates are not inferred observation dates. " + record["notes"]
            builder.fact(
                sid, metric, float(values[key]), "INR crore",
                f"PDF p{table['physical_page']}; printed p{table['printed_page']}; {table['statement']}; {column}",
                entity_id="state_" + re.sub(r"[^a-z0-9]+", "_", state.lower()),
                entity_name=state, entity_type="state_aggregate", agency=f"CAG / Government of {state}",
                state=state, road_class="roads_and_bridges_all_classes",
                start=f"{year}-04-01", end=f"{year+1}-03-31", basis="fiscal_year", estimate="actual",
                statement="state_finance_accounts_cash_expenditure_as_reported",
                reported_period=f"{year}-{str(year+1)[2:]}", published="", disclosure_as_of="",
                observation_status="final", assurance="audited_finance_accounts",
                revision_identity=record["sha256"], eligible=eligible, notes=notes,
            )
        builder.notes[sid] = (
            f"CAG {state} FY2024-25 Finance Accounts VolumeI; visually reviewed Statements4A/5; "
            "revenue and current/prior annual capital flows, original INRcrore, historical cutoffs retained. "
            "Publication date unknown. " + record["notes"]
        )


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Download/rebuild reviewed CAG state account flows")
    parser.add_argument("--raw-root", type=Path, default=Path("data/raw"))
    parser.add_argument("--download", action="store_true")
    parser.add_argument("--research-cutoff", default="2026-10-02")
    args = parser.parse_args()
    if args.download:
        print(json.dumps(download_documents(args.raw_root)))
    else:
        from pipelines.connectors.primary_disclosures import SnapshotBuilder
        builder = SnapshotBuilder(args.raw_root, args.research_cutoff)
        extend_snapshots(builder)
        builder.finish(SOURCE_IDS)
        print(json.dumps({sid: len(builder.rows[sid]) for sid in SOURCE_IDS}))
