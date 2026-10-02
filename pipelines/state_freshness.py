"""Source-pinned state statistical vintages and audited Roads/Bridges accounts.

This module extends the primary snapshot builder without replacing older sources.
Table observation periods, publication dates and geographical changes are kept
separate. Missing published cells never become zeros or forward-filled values.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlsplit

from pipelines.common import write_json

DOCUMENTS = {
    "rbi_gsdp_current_prices_2024_25": "https://rbidocs.rbi.org.in/rdocs/Publications/PDFs/21T_11122025D994949B48C44B68B4465FBB9ADDFF3D.PDF",
    "cag_karnataka_road_finances_2024_25": "https://cag.gov.in/uploads/state_accounts_report/account-report-2-Finance-Accounts-Vol-I-2024-25-government-of-karnataka-06948f82bb28817-62582478.pdf",
    "cag_maharashtra_road_finances_2024_25": "https://cag.gov.in/uploads/state_accounts_report/account-report-CD-ENGLISH-FINANCE-ACCOUNTS-VOL-I-06944f1e510c216-51074266.pdf",
    "cag_gujarat_road_finances_2024_25": "https://cag.gov.in/uploads/state_accounts_report/account-report-FA-VOL-I-2024-25-069c52aa2b34bf9-63690940.pdf",
    "cag_uttar_pradesh_road_finances_2024_25": "https://cag.gov.in/uploads/state_accounts_report/account-report-Finance-Accounts-Vol-I-2024-25-English-06964e82777f6d2-05539712.pdf",
    "cag_telangana_road_finances_2024_25": "https://cag.gov.in/uploads/state_accounts_report/account-report-Finance-Accounts-Volume-I-2024-25-069ce501b30f835-70020121.pdf",
}
SOURCE_IDS = tuple(DOCUMENTS)
PINNED_SHA256 = {
    "rbi_gsdp_current_prices_2024_25": "6e48293426935a2ef796c0163edbedeeb6dd165b21db8f018ac912502b3cc287",
    "cag_karnataka_road_finances_2024_25": "a60e4f514b0fa63dc3862db3de74c2c778b832bd09cc05341e5ec0381fadf9a5",
    "cag_maharashtra_road_finances_2024_25": "711cf4d8d72af4dbb87d75dca8974e6ba3785ed78bc24b145d775bd671742cbb",
    "cag_gujarat_road_finances_2024_25": "2c5c3b75ad46e7de9e40d24d871874a1e4c89b0c79f829f9cb7ca186d0aed46c",
    "cag_uttar_pradesh_road_finances_2024_25": "50343d23a580af96f455acaf086546d1282c571bb854055541771f99915eb411",
    "cag_telangana_road_finances_2024_25": "1c4ce5bab8dc25efcd6ae17cd28152e6bf11a1b184922d5ad782ef49b2fadcba",
}
GSDP_PUBLICATION = "2025-12-11"
GSDP_VINTAGE = "RBI Handbook of Statistics on Indian States 2024-25; Table 21; 11 December 2025"
ACCOUNT_PAGES = {
    "cag_karnataka_road_finances_2024_25": {"state": "Karnataka", "function": 41, "function_printed": 22, "capital": 51, "capital_printed": 32},
    "cag_maharashtra_road_finances_2024_25": {"state": "Maharashtra", "function": 29, "function_printed": 13, "capital": 34, "capital_printed": 18},
    "cag_gujarat_road_finances_2024_25": {"state": "Gujarat", "function": 33, "function_printed": 15, "capital": 39, "capital_printed": 21},
    "cag_uttar_pradesh_road_finances_2024_25": {"state": "Uttar Pradesh", "function": 24, "function_printed": 13, "capital": 30, "capital_printed": 19},
    "cag_telangana_road_finances_2024_25": {"state": "Telangana", "function": 35, "function_printed": 23, "capital": 47, "capital_printed": 35, "layout_reader": "pdfplumber"},
}
GSDP_MISSING_LATEST = {"Andaman and Nicobar Islands", "Chandigarh", "Goa", "Gujarat", "Ladakh", "Manipur", "Mizoram", "Nagaland", "Sikkim"}
JURISDICTIONS = ["Andaman and Nicobar Islands", "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chandigarh", "Chhattisgarh", "Dadra and Nagar Haveli and Daman and Diu", "Delhi", "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jammu and Kashmir", "Jharkhand", "Karnataka", "Kerala", "Ladakh", "Lakshadweep", "Madhya Pradesh", "Maharashtra", "Manipur", "Meghalaya", "Mizoram", "Nagaland", "Odisha", "Puducherry", "Punjab", "Rajasthan", "Sikkim", "Tamil Nadu", "Telangana", "Tripura", "Uttar Pradesh", "Uttarakhand", "West Bengal"]


def document_path(raw_root: Path, sid: str) -> Path:
    return raw_root / "state_freshness" / sid / "document.pdf"


class TableRows(HTMLParser):
    """Read cells, including empty ones, without an optional HTML dependency."""
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[str]] = []
        self.row: list[str] | None = None
        self.cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "tr":
            self.row = []
        if tag in {"td", "th"}:
            self.cell = []
        if tag == "br" and self.cell is not None:
            self.cell.append(" ")

    def handle_data(self, data: str) -> None:
        if self.cell is not None:
            self.cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self.cell is not None:
            if self.row is not None:
                self.row.append(" ".join("".join(self.cell).split()))
            self.cell = None
        if tag == "tr" and self.row is not None:
            self.rows.append(self.row)
            self.row = None


class Links(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[tuple[str, str]] = []
        self.href: str | None = None
        self.label: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "a":
            self.href = dict(attrs).get("href")
            self.label = []

    def handle_data(self, data: str) -> None:
        if self.href:
            self.label.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self.href:
            self.links.append((" ".join("".join(self.label).split()), self.href))
            self.href = None


def catalogue_finance_documents(content: str) -> dict[str, list[str]]:
    """Discovery only: account-year headings are not measurement cutoffs."""
    start = re.search(r'<div\s+id="tab-359"', content)
    if not start:
        return {}
    section = re.split(r'<div\s+id="tab-\d+"', content[start.end():], maxsplit=1)[0]
    # Regional CAG offices publish year headings as h5 rather than the central
    # accordion. Normalize only within the Finance Accounts tab; monthly or
    # appropriation links must not leak into financial-document discovery.
    section = re.sub(r"<h5>\s*(20\d{2})\s*-\s*(\d{2})\s*</h5>",
                     r'<div class="accTrigger">\1 - \2</div>', section)
    chunks = re.split(r'<div\s+class="accTrigger">\s*(20\d{2})\s*-\s*(\d{2})\s*</div>', section)
    results: dict[str, list[str]] = {}
    for offset in range(1, len(chunks), 3):
        year, suffix, chunk = chunks[offset:offset+3]
        if int(suffix) != (int(year)+1) % 100:
            raise ValueError("CAG account year heading invalid")
        parser = Links()
        parser.feed(chunk)
        urls = sorted({urljoin("https://cag.gov.in", url) for _, url in parser.links if urlsplit(url).path.lower().endswith(".pdf")})
        if urls:
            results[f"{year}-{suffix}"] = urls
    return results


def _retrieve_public_page(url: str, path: Path) -> dict[str, Any]:
    """Bounded, TLS-verified discovery fetch; record restricted outcomes honestly."""
    if urlsplit(url).scheme != "https":
        raise ValueError("Discovery requires HTTPS")
    hosts = {"cag.gov.in", "www.cag.gov.in", "nhit.co.in", "www.npci.org.in", "www.nhidcl.com"}
    if urlsplit(url).hostname not in hosts:
        raise ValueError("Unapproved discovery host")
    checked = datetime.now(timezone.utc).isoformat()
    try:
        request = urllib.request.Request(url, headers={"User-Agent": "BHAI-state-freshness/1.0"})
        with urllib.request.urlopen(request, timeout=25) as response:
            if urlsplit(response.url).scheme != "https" or urlsplit(response.url).hostname not in hosts:
                raise ValueError("Insecure discovery redirect")
            content = response.read(2_000_001)
            if len(content) > 2_000_000:
                raise ValueError("Discovery page exceeds limit")
            meta = {"url": url, "resolved_url": response.url, "http_status": response.status, "checked_at": checked, "sha256": hashlib.sha256(content).hexdigest(), "size_bytes": len(content), "outcome": "retrieved"}
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
            meta["capture_path"] = str(path)
            return meta
    except (OSError, ValueError, urllib.error.URLError) as exc:
        return {"url": url, "checked_at": checked, "outcome": "retrieval_blocked", "http_status": getattr(exc, "code", None), "reason": f"{type(exc).__name__}: {exc}"}


def audit_catalogues(raw_root: Path, output: Path) -> dict[str, Any]:
    """Audit all36 current jurisdictions, retaining absent/blocked index evidence."""
    capture_root = raw_root / "state_freshness/catalogue_audit"
    index = capture_root / "cag_state_index.html"
    index_meta = _retrieve_public_page("https://cag.gov.in/en/state-accounts-report?defuat_state_id=79", index)
    if index_meta["outcome"] != "retrieved":
        raise ValueError("CAG state selector unavailable; do not invent state URLs")
    parser = Links()
    parser.feed(index.read_text())
    state_links: dict[str, str] = {}
    territory_url = None
    for label, url in parser.links:
        if "Territor" in label:
            territory_url = urljoin("https://cag.gov.in", url)
        if "defuat_state_id=" not in url or label == "Archive":
            continue
        name = "Jammu and Kashmir" if label.startswith("Jammu and Kashmir State") else "Puducherry" if label == "Pondicherry" else canonical_state(label)
        if name in JURISDICTIONS:
            state_links[name] = urljoin("https://cag.gov.in", url)

    def check(state: str) -> dict[str, Any]:
        url = state_links.get(state)
        if not url:
            return {"jurisdiction": state, "status": "not_listed_in_state_selector", "reason": "No current-jurisdiction state-account selector found; investigate territory/Union accounts or local accountant-general index separately. No assumed source URL, release or zero expenditure.", "alternative_catalogue_url": territory_url, "finance_accounts": {}}
        path = capture_root / (re.sub(r"[^a-z0-9]+", "_", state.lower()) + ".html")
        metadata = _retrieve_public_page(url, path)
        reports = catalogue_finance_documents(path.read_text()) if metadata["outcome"] == "retrieved" else {}
        status = "account_files_discovered_extraction_pending" if reports else "retrieval_blocked" if metadata["outcome"] != "retrieved" else "no_finance_account_files_discovered"
        if state in {entry["state"] for entry in ACCOUNT_PAGES.values()} and "2024-25" in reports:
            status = "roads_bridges_fy2024_25_extracted"
        entry = {"jurisdiction": state, "status": status, "catalogue": metadata, "finance_accounts": reports, "latest_indexed_account_year": max(reports, default=None), "scope_review_required": state == "Jammu and Kashmir", "notes": "Discovery index uses a legacy pre30October2019 J&K selector label; current document scope must be verified." if state == "Jammu and Kashmir" else "Index proves document availability; no unextracted amount, publication date or observation cutoff inferred."}
        pending = raw_root / "state_freshness/cag_tamil_nadu_road_finances_2024_25/retrieval.json"
        if state == "Tamil Nadu" and pending.exists():
            entry["document_retrieval"] = json.loads(pending.read_text())
            entry["extraction_status"] = "pending_ocr_and_table_validation"
            entry["notes"] += " FY2024-25 Vol I PDF retrieved, but native font encoding and rotated table text prevent reliable row/column extraction. OCR and statement reconciliation remain pending; no numerical facts admitted."
        return entry

    with ThreadPoolExecutor(max_workers=4) as executor:
        jurisdictions = list(executor.map(check, JURISDICTIONS))
    payload = {"research_cutoff": "2026-10-02", "generated_at": datetime.now(timezone.utc).isoformat(), "index": index_meta, "jurisdiction_count": len(jurisdictions), "jurisdictions": jurisdictions, "method": "Observed official CAG state selectors; Finance Accounts tab359 and labelled account-year PDF links. Availability audit only; extract and validate each statement before numerical use.", "network_discovery": [{"source_url": "https://www.rbi.org.in/scripts/PublicationsView.aspx?id=23596", "latest_observation": "2020-03-31", "status": "older_than_existing_morth_2021_22", "note": "RecentDecember2025 handbook edition does not make its SH series current; some table cells explicitly useMarch2018 observations."}, {"source_url": "https://mahades.maharashtra.gov.in/files/publication/MaharashtrasEconomyinFigures2024.pdf", "latest_observation": None, "status": "retrieval_blocked_tls_hostname_mismatch", "note": "No numeric state network facts admitted without TLS-verified source retrieval and actual row-period/footnote checks."}]}
    write_json(payload, output)
    lines = ["# State publication availability audit", "", "Research cutoff:2October2026. Availability is distinct from extracted measurements. Unknown publication dates and missing jurisdictions remain explicit.", "", "| Jurisdiction | Latest indexed FinanceAccounts year | Status | Primary catalogue |", "|---|---|---|---|"]
    for entry in jurisdictions:
        url = entry.get("catalogue", {}).get("url") or entry.get("alternative_catalogue_url")
        citation = f"[CAG]({url})" if url else "No state selector found"
        lines.append(f"| {entry['jurisdiction']} | {entry.get('latest_indexed_account_year') or 'Unknown'} | {entry['status']} | {citation} |")
    lines += ["", "Audited FY2024-25 Roads/Bridges actuals are extracted and independently reconciled for: " + ", ".join(sorted(entry["state"] for entry in ACCOUNT_PAGES.values())) + ". Other state account links remain extraction-pending. Delhi/Puducherry/currentJ&K and territories need document/entity-scope review; state corporation debt is never inferred from these state-government accounts.", "", "Tamil Nadu FY2024-25 Vol I was retrieved. Native font encoding and a rotated table prevent reliable extraction; OCR and statement reconciliation remain pending, with no numerical facts admitted.", "", "The RBI December2025 SH table endsMarch2020 and does not supersede existing MoRTHMarch2022 stock. Maharashtra DES candidate failed TLS hostname validation; no network amount/date admitted.", ""]
    output.with_suffix(".md").write_text("\n".join(lines))
    return payload


def audit_monthly_publications(raw_root: Path, output: Path) -> dict[str, Any]:
    checks = []
    for source, url, last_verified in [
        ("nhit", "https://nhit.co.in/reports/", "2026-06-30"),
        ("npci_netc", "https://www.npci.org.in/product/netc/product-statistics", "2026-08-31"),
        ("nhidcl", "https://www.nhidcl.com/en/current-status", "2026-08-31"),
    ]:
        path = raw_root / f"state_freshness/publication_checks/{source}.html"
        check = _retrieve_public_page(url, path)
        check.update(source_id=source, last_verified_observation=last_verified, newer_publication_verified=False, publication_date=None)
        if check["outcome"] == "retrieved":
            parser = Links()
            parser.feed(path.read_text())
            check["candidate_links"] = [{"label": label, "url": urljoin(url, href)} for label, href in parser.links if any(token in (label + " " + href).lower() for token in ["2026", "2026-09", "2026-10"])][:100]
            check["status"] = "index_retrieved_document_period_verification_pending"
            check["note"] = "Index/filing/record-date/trading-window dates do not prove an operational reporting-period update. A browser index may differ from HTTP cached content; no claim that unobserved results are unreleased."
        else:
            check["status"] = "latest_publication_check_blocked"
            check["note"] = "A restricted response does not prove that September data are unavailable or unreleased. Retain the existing verified observation cutoff and request primary rendered-table verification."
        checks.append(check)
    payload = {"research_cutoff": "2026-10-02", "checks": checks, "method": "TLS-verified official index discovery; publisher metadata/footer is not measurement evidence. No inferred September2026 monthly values or Q2FY27 financial results."}
    rendered = Path(__file__).resolve().parents[1] / "research/rendered_publication_checks_2026_10_02.json"
    if rendered.exists():
        recheck = json.loads(rendered.read_text())
        payload["rendered_listing_recheck"] = {"evidence_path": "research/rendered_publication_checks_2026_10_02.json", "sha256": hashlib.sha256(rendered.read_bytes()).hexdigest(), "checked_at": recheck["checked_at"], "method": recheck["method"], "scope": recheck["scope"]}
        for check in checks:
            key = "npci" if check["source_id"] == "npci_netc" else check["source_id"]
            if key in recheck:
                check["rendered_verification"] = {"checked_at": recheck["checked_at"], "url": recheck[key]["url"], "latest_displayed_month": recheck[key].get("latest_month"), "latest_displayed_financial_quarter_end": recheck[key].get("latest_financial_quarter_end"), "september_status": recheck[key]["september_status"], "scope": "Listing/table recheck only; original observation, publication and snapshot retrieval dates unchanged."}
                if key == "npci":
                    check["rendered_verification"]["exclusions"] = recheck[key]["exclusions"]
    write_json(payload, output)
    return payload


def canonical_state(label: str) -> str:
    return label.rstrip("*").strip().replace("&", "and")


def parse_gsdp(content: str) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    """Parse both table spreads; header-to-cell positions are never shifted."""
    parser = TableRows()
    parser.feed(content)
    if not all(anchor in content for anchor in ["Gross State Domestic Product (Current Prices)", "Dec 11, 2025", "Base: 2011-12", "Lakh", "2018-19", "UT of Jammu and Kashmir"]):
        raise ValueError("RBI GSDP edition, units or geographic footnote changed")
    return parse_gsdp_rows(parser.rows)


def parse_gsdp_rows(rows: list[list[str]]) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    years: list[str] = []
    seen_headers: list[list[str]] = []
    observations: list[dict[str, Any]] = []
    missing: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        if row and all(re.fullmatch(r"20\d{2}-\d{2}", cell) for cell in row):
            years = row
            seen_headers.append(row)
            continue
        if not years or len(row) != len(years) + 1:
            continue
        if not any(re.fullmatch(r"[\d,]+|[-–]", cell) for cell in row[1:]):
            continue
        state = canonical_state(row[0])
        for fiscal, value in zip(years, row[1:]):
            year = int(fiscal[:4])
            geography = "Jammu and Kashmir (including Ladakh)" if state == "Jammu and Kashmir" and year <= 2018 else state
            key = (geography, fiscal)
            if key in seen:
                raise ValueError("Duplicate state/year in RBI vintage")
            seen.add(key)
            if value in {"", "-", "–"}:
                missing.append({"state": geography, "reported_period": fiscal, "reason": "published_cell_unavailable"})
                continue
            if not re.fullmatch(r"[\d,]+", value):
                raise ValueError(f"Ambiguous RBI GSDP cell: {state}/{fiscal}/{value}")
            observations.append({"state": geography, "reported_period": fiscal, "original_value": float(value.replace(",", "")), "period_start": f"{year}-04-01", "period_end": f"{year+1}-03-31"})
    expected = [[f"{y}-{str(y+1)[2:]}" for y in range(2011, 2017)], [f"{y}-{str(y+1)[2:]}" for y in range(2017, 2025)]]
    if seen_headers != expected:
        raise ValueError("RBI GSDP year column contract changed")
    if {item["state"] for item in missing if item["reported_period"] == "2024-25"} != GSDP_MISSING_LATEST:
        raise ValueError("RBI latest-period missing coverage changed; review edition")
    return observations, missing


def parse_gsdp_pdf(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    from pypdf import PdfReader
    reader = PdfReader(path)
    if len(reader.pages) != 2:
        raise ValueError("RBI Table21 PDF page contract changed")
    rows: list[list[str]] = []
    for index, page in enumerate(reader.pages):
        text = page.extract_text()
        if not all(anchor in text for anchor in ["TABLE 21:", "Current Prices", "Lakh", "Base: 2011-12"]):
            raise ValueError("RBI GSDP PDF units/price-basis contract changed")
        years = range(2011, 2017) if index == 0 else range(2017, 2025)
        rows.append([f"{year}-{str(year+1)[2:]}" for year in years])
        count = len(rows[-1])
        for line in text.splitlines():
            match = re.fullmatch(r"(.+?)\s+((?:[\d,]+|-)(?:\s+(?:[\d,]+|-)){" + str(count-1) + r"})", line.strip())
            if match:
                rows.append([match[1], *match[2].split()])
    if "UT of Jammu and Kashmir" not in " ".join(reader.pages[1].extract_text().split()):
        raise ValueError("RBI GSDP PDF geographical footnote changed")
    return parse_gsdp_rows(rows)


def _numeric_tokens(text: str) -> list[float]:
    return [(-1 if sign else 1) * float(value.replace(",", "")) for sign, value in re.findall(r"(?<![\d.])(\(-\)\s*)?([\d,]+\.\d{2})(?!\d)", text)]


def parse_account_pages(function_text: str, capital_text: str) -> dict[str, float]:
    """Cross-check annual capital outlay in two independently labelled tables."""
    function_text = " ".join(function_text.split())
    capital_text = " ".join(capital_text.split())
    capital_text = capital_text.replace("2023-2024", "2023-24").replace("2024-2025", "2024-25").replace("2024 -25", "2024-25")
    if not all(token in function_text for token in ["Revenue", "Capital", "Total", "Roads and Bridges"]):
        raise ValueError("CAG functional expenditure headings changed")
    if not any(token in function_text for token in ["Loan", "L & A"]):
        raise ValueError("CAG loans/advances heading missing")
    row = re.search(r"Roads and Bridges(.*?)Road Transport", function_text, re.S)
    capital = re.search(r"5054\s*-?\s*Capital Outlay on Roads and Bridges(.*?)5055", capital_text, re.S)
    if row is None or capital is None or not all(year in capital_text for year in ["2023-24", "2024-25"]):
        raise ValueError("CAG Roads/Bridges page contract changed")
    functional = _numeric_tokens(row.group(1))
    progressive = _numeric_tokens(capital.group(1))
    if len(progressive) == 7 and all(label in capital_text for label in ["un-apportioned expenditure", "allocated to", "Telangana"]):
        # In this labelled layout the bold sub-row reports unchanged legacy
        # cumulative balances. It has no annual-flow or allocated amount.
        if progressive[-2] != progressive[-1]:
            raise ValueError("CAG unapportioned legacy balance changed; review row geometry")
        progressive = progressive[:5]
    if len(functional) not in {3, 4} or len(progressive) != 5:
        raise ValueError("Unexpected CAG amount columns; no guessed column positions")
    revenue, current, total = functional[0], functional[1], functional[-1]
    previous, _, corroboration, _, _ = progressive
    if not math.isclose(current, corroboration, abs_tol=.005) or not math.isclose(sum(functional[:-1]), total, abs_tol=.015):
        raise ValueError("CAG capital cross-table reconciliation failed")
    return {"revenue_2024_25": revenue, "capital_2024_25": current, "capital_2023_24": previous}


def _verify_pinned(path: Path, sid: str) -> None:
    if hashlib.sha256(path.read_bytes()).hexdigest() != PINNED_SHA256[sid]:
        raise ValueError(f"{sid}: changed primary document requires reviewed re-extraction")


def extend_snapshots(builder: Any) -> None:
    """Integration hook called by the primary builder; only local pinned inputs."""
    for sid in SOURCE_IDS:
        builder.rows.setdefault(sid, [])
        builder.documents.setdefault(sid, [])
        path = document_path(builder.raw_root, sid)
        if not path.exists():
            builder.notes[sid] = "Pinned primary document unavailable; no numerical facts admitted."
            continue
        _verify_pinned(path, sid)
        if not builder.documents[sid]:
            builder.pin(sid, path, DOCUMENTS[sid])
        if sid.startswith("rbi_"):
            records, missing = parse_gsdp_pdf(path)
            index = path.parent / "document.html"
            if index.exists():
                html_records, html_missing = parse_gsdp(index.read_text())
                if records != html_records or missing != html_missing:
                    raise ValueError("RBI Table21 PDF and publication index disagree")
                builder.documents[sid][-1].update(publication_evidence_url="https://www.rbi.org.in/scripts/PublicationsView.aspx?id=23470", publication_evidence_sha256=hashlib.sha256(index.read_bytes()).hexdigest())
            for record in records:
                historical = record["state"].endswith("(including Ladakh)")
                builder.fact(sid, "gsdp_current_prices_inr_crore", record["original_value"], "INR crore", f"Table 21, state {record['state']}, column {record['reported_period']}", entity_id="state_" + re.sub(r"[^a-z0-9]+", "_", record["state"].lower()).strip("_"), entity_name=record["state"], entity_type="historical_state_aggregate" if historical else "state_aggregate", agency="RBI / NSO", state=record["state"], road_class="All sectors", start=record["period_start"], end=record["period_end"], basis="fiscal_year", estimate="actual", statement="GSDP_current_prices_2011_12_base", published=GSDP_PUBLICATION, disclosure_as_of=GSDP_PUBLICATION, reported_period=record["reported_period"], original_unit="INR lakh", observation_status="reported", assurance="official_statistical_published", revision_identity=GSDP_VINTAGE, price_basis="current_prices", base_year="2011-12", notes="Full RBI December2025 published vintage; values may revise earlier releases. Statistical estimates for completed reporting periods, not audited financial actuals; individual AE/RE/provisional status is not specified in this table. Missing cells omitted, never zero-filled. J&K throughFY2018-19 includes Ladakh; retained as a separate historical geography and not joined to present UT totals.")
            summary = {"source_id": sid, "revision_identity": GSDP_VINTAGE, "row_count": len(records), "missing_cells": missing, "latest_period": "2024-25", "latest_period_states_with_values": sorted(record["state"] for record in records if record["reported_period"] == "2024-25"), "jurisdictions_absent_from_table": ["Dadra and Nagar Haveli and Daman and Diu", "Lakshadweep"], "publication_date": GSDP_PUBLICATION}
            write_json(summary, path.parent / "extraction_summary.json")
            builder.notes[sid] = "RBI Table21 full2011-12–2024-25 December2025 vintage. Missing cells and historical J&K/Ladakh boundary preserved; original INRlakh normalized by0.01 to INRcrore. Completed-period statistical estimates have undisclosed revision/assurance statuses distinct from audited accounts."
            continue
        from pypdf import PdfReader
        contract = ACCOUNT_PAGES[sid]
        reader = PdfReader(path)
        if contract.get("layout_reader") == "pdfplumber":
            import pdfplumber
            with pdfplumber.open(path) as document:
                function_text = document.pages[contract["function"] - 1].extract_text()
                capital_text = document.pages[contract["capital"] - 1].extract_text()
        else:
            function_text = reader.pages[contract["function"] - 1].extract_text()
            capital_text = reader.pages[contract["capital"] - 1].extract_text()
        values = parse_account_pages(function_text, capital_text)
        state = contract["state"]
        for metric, key, year, page, printed, table in [
            ("roads_bridges_revenue_expenditure_inr_crore", "revenue_2024_25", 2024, contract["function"], contract["function_printed"], "Statement4A functional expenditure, Roads and Bridges, Revenue"),
            ("roads_bridges_capital_outlay_inr_crore", "capital_2024_25", 2024, contract["capital"], contract["capital_printed"], "Statement5, major head5054, expenditure during2024-25"),
            ("roads_bridges_capital_outlay_inr_crore", "capital_2023_24", 2023, contract["capital"], contract["capital_printed"], "Statement5, major head5054, expenditure during2023-24 comparative"),
        ]:
            notes = "Audited state-government cash accounts, functional Roads and Bridges covering multiple road classes; not State Highway-only spending or a corporation's financial statement. Excludes separate Road Transport head5055. Annual flow is distinct from the neighbouring progressive/cumulative expenditure column. Publication/signing date is not source-supported here and stays unknown; retrieval date never becomes the cutoff. Current capital independently reconciled to Statement4A."
            if state == "Karnataka":
                notes += " Statement4B printedp27 notes INR92.59crore interest on off-budget borrowing under MH3054; finance cost is not automatically construction expenditure."
            if state == "Telangana":
                notes += " Statement5 contains a separate allocation/unapportioned legacy column; that column and cumulative expenditure are not added to annual spending."
            builder.fact(sid, metric, values[key], "INR crore", f"PDF p{page}; printed p{printed}; {table}", entity_id="state_" + state.lower().replace(" ", "_"), entity_name=state, entity_type="state_aggregate", agency=f"CAG / Government of {state}", state=state, road_class="roads_and_bridges_all_classes", start=f"{year}-04-01", end=f"{year+1}-03-31", basis="fiscal_year", estimate="actual", statement="state_finance_accounts_cash_expenditure_as_reported", reported_period=f"{year}-{str(year+1)[2:]}", published="", observation_status="final", assurance="audited_finance_accounts", revision_identity="Finance Accounts2024-25 published vintage", notes=notes)
        write_json({"source_id": sid, "state": state, "source_as_of_date": "2025-03-31", "publication_date": None, "values_inr_crore": values, "reconciliation": "current-year capital agrees between Statement4A and Statement5; annual flow selected instead of cumulative", "row_count": 3}, path.parent / "extraction_summary.json")
        builder.notes[sid] = f"CAG {state} FY2024-25 audited accounts; revenue and capital Roads/Bridges amounts validated against labelled pages. FY2023-24 capital comparative retained. Publication date unknown; excludes corporation/SH-only attribution."


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Rebuild pinned state disclosures or audit official publication indexes")
    parser.add_argument("--raw-root", type=Path, default=Path("data/raw"))
    parser.add_argument("--audit", action="store_true", help="Discover FinanceAccounts and near-current monthly publication availability")
    args = parser.parse_args()
    if args.audit:
        state_audit = audit_catalogues(args.raw_root, Path("research/state_publication_audit_2026_10_02.json"))
        monthly = audit_monthly_publications(args.raw_root, Path("research/monthly_publication_checks_2026_10_02.json"))
        print(json.dumps({"jurisdictions": len(state_audit["jurisdictions"]), "monthly_checks": len(monthly["checks"])}))
    else:
        from pipelines.connectors.primary_disclosures import SnapshotBuilder
        snapshot = SnapshotBuilder(args.raw_root)
        extend_snapshots(snapshot)
        snapshot.finish(SOURCE_IDS)
        print(json.dumps({sid: len(snapshot.rows[sid]) for sid in SOURCE_IDS}))
