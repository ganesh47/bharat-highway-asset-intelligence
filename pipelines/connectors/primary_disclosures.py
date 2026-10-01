"""Verified, source-pinned financial and operational disclosure snapshots.

Publication discovery is separate from fact extraction. A document changing its
checksum cannot silently retain an extract produced from the previous document.
The snapshot builder is deliberately explicit about pages, units and scope.
"""
from __future__ import annotations

import argparse
import calendar
import csv
import hashlib
from html import unescape
import json
import math
import os
import re
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pandas as pd

from .base import ConnectorResult, ConnectorSpec
from pipelines.common import ensure_dirs, sha256_for_file, write_json, write_parquet
from pipelines.quality import evaluate

FACT_COLUMNS = [
    "entity_id", "entity_name", "entity_type", "agency", "state", "road_class",
    "metric", "value", "unit", "original_value", "original_unit", "period_start",
    "period_end", "period_basis", "estimate_type", "statement_basis", "data_as_of",
    "published_at", "source_id", "citation_url", "table_page", "evidence_class",
    "analytical_eligible", "source_document_sha256", "notes", "entity", "year",
    "metric_name", "metric_value", "metric_category", "source_type",
    "asset_id", "project_name", "nh_number", "project_stage",
    "scheduled_completion_date", "likely_completion_date", "actual_completion_date",
    "comparison_group",
    "asset_owner_id", "implementing_agency_id", "operator_id",
    "concessionaire_id", "financing_entity_id", "contractor_name",
    "disclosure_as_of", "estimate_vintage", "reported_period",
]

# These are documented public downloads, not guessed portal/API endpoints.
DOCUMENTS = {
    "union_budget_morth_demand86": "https://www.indiabudget.gov.in/doc/eb/sbe86.pdf",
    "union_budget_highway_outcomes": "https://www.indiabudget.gov.in/doc/OutcomeBudgetE2026_2027.pdf",
    "nhit_quarterly_operations_finance": "https://nhit.co.in/pdf/investor-presentations/Investor%20Presentation%20June%202026.pdf",
    "nhit_quarterly_financial_filings": "https://nhit.co.in/pdf/communication-to-stock-exchange/BM%20Outcome%2007082026%20F.pdf",
    "nhit_asset_valuation_assumptions": "https://nhit.co.in/pdf/valuation-report/Valuation%20Report%20F_08_08_26.pdf",
    "nhit_annual_report": "https://nhit.co.in/pdf/annual-report/NHIT_Annual%20Report%20FY%202025-26.pdf",
    "nhidcl_monthly_project_progress": "https://www.nhidcl.com/sites/default/files/2026-09/nhidcl-completed_and_ongoing_projects_status_per_pmp_data_lake_portal_31-08-2026.pdf",
    "parliament_nhai_debt_tot_invit": "https://sansad.in/getFile/loksabhaquestions/annex/187/AU963_8iM1pC.pdf?source=pqals",
    "nhai_monetisation_transactions": "https://www.pib.gov.in/PressReleaseIframePage.aspx?PRID=2247011&lang=2&reg=48",
    "npci_netc_monthly_statistics": "https://www.npci.org.in/product/netc/product-statistics",
    "rbi_state_road_finances": "https://rbidocs.rbi.org.in/rdocs/Publications/PDFs/0SF23012026877D47254C4F4B0793B2C38F05FB7EC5.PDF",
    "cag_bharatmala_performance_audit": "https://saiindia.gov.in/webroot/uploads/download_audit_report/2023/Report-No.-19-of-203--Bharatmala-English-064d5db7bc63c20.06754442.pdf",
    "morth_state_highway_network": "https://morth.gov.in/backend/documents/uploaded/1781850176_EDsilQvsKL.pdf",
    "upeida_expressway_projects": "https://upeida.up.gov.in/en/page/ganga-expressway",
    "msrdc_financial_disclosures": "https://msrdc.in/1262/Annual-Reports-and-Details-of-Bonds?format=print",
    "adb_state_road_projects": "https://www.adb.org/sites/default/files/project-documents/52298/52298-001-apfs-en_3.pdf",
}
SOURCE_IDS = tuple(DOCUMENTS)
HTML_SOURCES = {"nhai_monetisation_transactions", "npci_netc_monthly_statistics", "upeida_expressway_projects", "msrdc_financial_disclosures"}
HTML_RECHECK_SOURCES = {"nhai_monetisation_transactions"}
RESEARCH_CUTOFF = "2026-10-02"
NETC_SOURCE_ID = "npci_netc_monthly_statistics"
NETC_RENDERED_SNAPSHOT = Path("manual/evidence/npci_netc_rendered_snapshot_2026-10-02.json")
RENDERED_SNAPSHOT_KIND = "governed_rendered_table_snapshot"
ADB_PROJECT_PDF_SHA256 = "d8d949023e38e46929f39edf948b642fcc670d34332e841ba186121d0b955ffd"


def document_path(raw_root: Path, source_id: str) -> Path:
    return raw_root / "primary_disclosures" / source_id / ("document.html" if source_id in HTML_SOURCES else "document.pdf")


def download_document(url: str, destination: Path, max_bytes: int = 80_000_000) -> Path:
    """Bounded TLS-verified download; HTML challenge pages never become PDFs."""
    if urlsplit(url).scheme != "https":
        raise ValueError("Primary document requires HTTPS")
    request = urllib.request.Request(url, headers={"User-Agent": "BHAI-primary-research/1.0"})
    with urllib.request.urlopen(request, timeout=45) as response:
        if urlsplit(response.url).scheme != "https":
            raise ValueError("Non-HTTPS redirect")
        if urlsplit(response.url).hostname != urlsplit(url).hostname:
            raise ValueError("Unapproved cross-host redirect")
        length = response.headers.get("Content-Length")
        if length and int(length) > max_bytes:
            raise ValueError("Document exceeds size limit")
        content = response.read(max_bytes + 1)
    if len(content) > max_bytes:
        raise ValueError("Document exceeds size limit")
    if destination.suffix == ".pdf" and not content.startswith(b"%PDF-"):
        raise ValueError("Response is not a PDF")
    if destination.suffix == ".html" and any(token in content.lower() for token in [b"what code is in the image", b"access denied", b"verify you are human", b"please enable javascript to view the page content"]):
        raise ValueError("Restricted/challenge response")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    temporary.write_bytes(content)
    temporary.replace(destination)
    return destination


def empty_facts() -> pd.DataFrame:
    df = pd.DataFrame({column: pd.Series(dtype="string") for column in FACT_COLUMNS})
    for column in ["value", "original_value", "metric_value"]:
        df[column] = pd.Series(dtype="float64")
    df["analytical_eligible"] = pd.Series(dtype="bool")
    df["year"] = pd.Series(dtype="Int64")
    return df


def validate_facts(df: pd.DataFrame, source_id: str, evidence: dict[str, Any]) -> pd.DataFrame:
    missing = set(FACT_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Missing fact columns: {sorted(missing)}")
    if df.empty:
        return empty_facts()
    df = df.copy()
    document_hashes = {item["sha256"] for item in evidence.get("documents", []) if re.fullmatch(r"[0-9a-f]{64}", item.get("sha256", ""))}
    urls = {item["url"] for item in evidence.get("documents", [])}
    document_pairs = {(item["sha256"],item["url"]) for item in evidence.get("documents", [])}
    if not document_hashes:
        raise ValueError("No source document checksum")
    if not df["source_id"].eq(source_id).all():
        raise ValueError("Mixed source IDs")
    for column in ["entity_id", "entity_type", "agency", "road_class", "metric", "unit", "period_basis", "estimate_type", "statement_basis", "citation_url", "table_page", "evidence_class"]:
        if df[column].fillna("").astype(str).str.strip().eq("").any():
            raise ValueError(f"Missing fact lineage: {column}")
    for column in ["value", "original_value", "metric_value"]:
        df[column] = pd.to_numeric(df[column], errors="raise")
        if not df[column].map(math.isfinite).all():
            raise ValueError("Non-finite numerical fact")
    if not df["source_document_sha256"].isin(document_hashes).all() or not df["citation_url"].isin(urls).all():
        raise ValueError("Fact/document lineage mismatch")
    if any((row.source_document_sha256,row.citation_url) not in document_pairs for row in df.itertuples()):
        raise ValueError("Fact source URL/checksum pair mismatch")
    for column in ["data_as_of", "published_at", "disclosure_as_of", "estimate_vintage"]:
        values = df[column].fillna("").astype(str)
        for value in values[values.ne("")]:
            if date.fromisoformat(value) > date.fromisoformat(RESEARCH_CUTOFF):
                raise ValueError("Fact exceeds research cutoff")
    if not df["value"].equals(df["metric_value"]):
        raise ValueError("Metric compatibility alias mismatch")
    money = df["unit"].eq("INR crore")
    conversions = {"INR crore": 1.0, "INR lakh": 0.01, "INR million": 0.1, "INR thousand": 0.0001}
    for _, row in df[money].iterrows():
        factor = conversions.get(row["original_unit"])
        if factor is None or not math.isclose(row["value"], row["original_value"] * factor, rel_tol=1e-9, abs_tol=1e-8):
            raise ValueError("Invalid monetary unit conversion")
    transactions = df["unit"].eq("transactions") | df["original_unit"].eq("million transactions") | df["metric"].eq("netc_payment_transactions")
    for _, row in df[transactions].iterrows():
        factor = {"transactions": 1.0, "million transactions": 1_000_000.0}.get(row["original_unit"])
        if row["unit"] != "transactions" or factor is None or not math.isclose(row["value"], row["original_value"] * factor, rel_tol=1e-9, abs_tol=1e-8):
            raise ValueError("Invalid transaction unit conversion")
    df["analytical_eligible"] = df["analytical_eligible"].map(lambda value: str(value).lower() == "true")
    if df.duplicated(["source_id","entity_id","metric","period_start","period_end","estimate_type","statement_basis"]).any():
        raise ValueError("Duplicate scoped numerical facts")
    if (df["analytical_eligible"] & df["evidence_class"].isin(["target","valuation_estimate"])).any():
        raise ValueError("Targets and valuation assumptions cannot enter measured analytics")
    if (df["analytical_eligible"] & df["data_as_of"].fillna("").eq("")).any():
        raise ValueError("Analytical facts require a source observation date")
    progress=df["metric"].str.endswith("progress_percent")
    if ((df.loc[progress,"value"]<0)|(df.loc[progress,"value"]>100)).any():
        raise ValueError("Progress outside 0-100")
    for _,row in df.iterrows():
        if row["period_start"] and row["period_end"] and date.fromisoformat(row["period_start"])>date.fromisoformat(row["period_end"]):
            raise ValueError("Reversed fact period")
        if row["estimate_type"] in {"actual","YTD"} and row["period_end"] and row["period_end"]>RESEARCH_CUTOFF:
            raise ValueError("Future actual observation")
        if row["estimate_type"] in {"actual", "YTD"} and row["period_end"] and row["data_as_of"] and row["data_as_of"] > row["period_end"]:
            raise ValueError("Observation cutoff exceeds actual reporting period; use disclosure_as_of for later assertions")
        if row["analytical_eligible"] and row["estimate_type"] in {"BE", "RE"} and (not row["estimate_vintage"] or row["estimate_vintage"] != row["data_as_of"]):
            raise ValueError("Analytical estimates require an explicit source-supported vintage")
    return df


class PrimaryDisclosuresConnector:
    spec = ConnectorSpec(name="primary_disclosures", version="1.0.0", source_ids=list(SOURCE_IDS), inputs=["verified_primary_snapshot"], outputs=["parquet"], citation_mapping={"primary_source": "citation_url", "anchor": "table_page", "permanent_identifier": "source_document_sha256", "license_terms": "license_terms"})

    def run(self, source: dict[str, Any], raw_root: Path, processed_root: Path, manifest_root: Path) -> ConnectorResult:
        source_id = source["source_id"]
        csv_path = raw_root / "manual" / f"{source_id}.csv"
        evidence_path = raw_root / "manual" / "evidence" / f"{source_id}.json"
        now = datetime.now(timezone.utc).isoformat()
        rows = empty_facts()
        evidence: dict[str, Any] = {}
        reason = "validated_extract_missing"
        raw_files = []
        checked_retrieval_at = None
        semantic_rechecks = []
        if evidence_path.exists():
            evidence = json.loads(evidence_path.read_text())
            raw_files.append(evidence_path)
            reason = evidence.get("gap_reason") or reason
        if csv_path.exists() and evidence.get("extraction_status") == "validated":
            try:
                if sha256_for_file(csv_path) != evidence.get("csv_sha256"):
                    raise ValueError("Extract checksum mismatch")
                # If restored, the archived primary document must match the extract.
                for document in evidence.get("documents", []):
                    path = raw_root / document["relative_path"]
                    if document.get("artifact_kind") == RENDERED_SNAPSHOT_KIND and not path.is_file():
                        raise ValueError("Governed rendered snapshot missing")
                    if path.exists():
                        if sha256_for_file(path) != document["sha256"]:
                            raise ValueError("Primary document changed; re-extraction required")
                        raw_files.append(path)
                # A daily refresh checks accessible primary bytes. Candidate bytes
                # cannot overwrite the archived document behind validated rows.
                if source.get("allow_auto_fetch") and os.environ.get("BHAI_PRIMARY_REMOTE_CHECK", "1") != "0":
                    for document in evidence.get("documents", []):
                        if document.get("artifact_kind") == RENDERED_SNAPSHOT_KIND:
                            # The pinned bytes are a governed local capture, not
                            # the publisher response at the cited page URL.
                            continue
                        pinned = raw_root / document["relative_path"]
                        candidate = pinned.with_name("candidate" + pinned.suffix)
                        check_path = pinned.parent / "remote_check.json"
                        previous = json.loads(check_path.read_text()) if check_path.exists() else {}
                        if previous.get("checked_date") == now[:10] and previous.get("outcome") == "checked_unchanged" and previous.get("pinned_sha256",previous.get("sha256")) == document["sha256"]:
                            checked_retrieval_at=previous.get("checked_at")
                            continue
                        try:
                            download_document(document["url"], candidate)
                            checked_retrieval_at = now
                            current_hash = sha256_for_file(candidate)
                            check={"checked_date":now[:10],"checked_at":now,"sha256":current_hash,"pinned_sha256":document["sha256"],"url":document["url"],"outcome":"checked_unchanged" if current_hash==document["sha256"] else "changed_document_requires_extraction"}
                            if current_hash != document["sha256"]:
                                # Known PIB HTML adds dynamic script/viewstate
                                # wrappers. Re-extract all visible source text and
                                # compare its durable digest; never rebind CSVs.
                                if source_id in HTML_RECHECK_SOURCES and candidate.suffix==".html":
                                    semantic_hash=hashlib.sha256(html_text(candidate.read_text()).encode()).hexdigest()
                                    check["semantic_document_sha256"]=semantic_hash
                                    if semantic_hash==document.get("semantic_document_sha256"):
                                        archive=pinned.parent/"wrapper_archive"/(current_hash+pinned.suffix)
                                        archive.parent.mkdir(parents=True,exist_ok=True)
                                        candidate.replace(archive)
                                        check.update(outcome="checked_unchanged",archive_path=str(archive))
                                        semantic_rechecks.append(check)
                                        raw_files.append(archive)
                                        write_json(check,check_path)
                                        continue
                                quarantine=pinned.parent/"quarantine"/(current_hash+pinned.suffix)
                                quarantine.parent.mkdir(parents=True,exist_ok=True)
                                candidate.replace(quarantine)
                                write_json(check,check_path)
                                raise ValueError("changed_document_requires_extraction")
                            write_json(check,check_path)
                        except (OSError, urllib.error.URLError) as exc:
                            raise ValueError(f"failed_retrieval: {type(exc).__name__}") from exc
                        finally:
                            if candidate.exists():
                                candidate.unlink()
                rows = validate_facts(pd.read_csv(csv_path, keep_default_na=False), source_id, evidence)
                raw_files.append(csv_path)
                reason = "" if not rows.empty else "no_validated_numeric_facts"
            except (ValueError, KeyError, TypeError) as exc:
                reason = str(exc)
                rows = empty_facts()
        output = processed_root / f"{source_id}.parquet"
        ensure_dirs(str(processed_root), str(manifest_root))
        write_parquet(rows, output)
        validated = not rows.empty
        ready = validated and bool(rows["analytical_eligible"].any())
        manifest = {
            "source_id": source_id, "connector": self.spec.name, "version": self.spec.version,
            "status": "manual_ingest" if validated else "manual_gap", "metric_category": source.get("metric_category", "issuer_disclosed" if source.get("publisher_type") == "issuer" else "official_measured"),
            "refresh_outcome": "checked_unchanged" if ready else "unavailable", "last_checked_at": now,
            "last_successful_retrieval_at": checked_retrieval_at or evidence.get("retrieved_at"), "data_changed_at": evidence.get("retrieved_at"),
            "publication_date": source.get("publication_date"), "source_as_of_date": source.get("source_as_of_date"),
            "analytical_ready": ready, "evidence_status": "verified" if validated else "unavailable",
            "disclosure_ready": validated,
            "extraction_status": "validated" if validated else evidence.get("extraction_status", "missing"), "skip_reason": reason,
            "source": {"publisher": source.get("publisher_org"), "title": source.get("dataset_title"), "url": source.get("url"), "retrieved_at": evidence.get("retrieved_at"), "official_flag": source.get("official_flag", True), "publisher_type": source.get("publisher_type", "government"), "license_terms": source.get("license_terms")},
            "citations": {"permanent_identifier": source_id, "anchor": "row.citation_url + row.table_page + row.source_document_sha256", "note": evidence.get("notes", reason)},
            "manifest": {"raw_files": [{"path": str(path), "sha256": sha256_for_file(path), "size_bytes": path.stat().st_size} for path in raw_files], "source_documents": evidence.get("documents", []), "output_files": [{"path": str(output), "format": "parquet", "sha256": sha256_for_file(output)}], "row_count": len(rows), "columns": list(rows.columns)},
            "retrieved_at": evidence.get("retrieved_at"),
            "semantic_rechecks": semantic_rechecks,
        }
        if ready:
            manifest.update(evaluate(rows, source | manifest["source"]))
        write_json(manifest, manifest_root / f"{source_id}.json")
        return ConnectorResult(source_id, output, manifest, skipped=not validated, skip_reason=reason or None)


def number(value: str) -> float:
    return float(str(value).replace(",", "").strip().rstrip("x"))


def fiscal_period(start: int) -> tuple[str, str]:
    return f"{start}-04-01", f"{start + 1}-03-31"


def identifier(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def html_text(content: str) -> str:
    content = re.sub(r"<(script|style)\b[^>]*>.*?</\1>", " ", content, flags=re.S|re.I)
    return re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", content))).strip()


class SnapshotBuilder:
    """Rebuild governed CSVs from primary documents with explicit table contracts.

    Callers download once, archive source bytes, then extract offline. Assertions
    deliberately fail on changed tables rather than guessing new column layouts.
    """
    def __init__(self, raw_root: Path):
        self.raw_root = raw_root
        self.rows: dict[str, list[dict[str, Any]]] = {sid: [] for sid in SOURCE_IDS}
        self.documents: dict[str, list[dict[str, Any]]] = {sid: [] for sid in SOURCE_IDS}
        self.notes: dict[str, str] = {}
        self.reader_cache: dict[str, Any] = {}

    def reader(self, sid: str):
        from pypdf import PdfReader
        if sid not in self.reader_cache:
            self.reader_cache[sid] = PdfReader(document_path(self.raw_root, sid))
        return self.reader_cache[sid]

    def text(self, sid: str, page: int) -> str:
        return self.reader(sid).pages[page - 1].extract_text()

    def pin(self, sid: str, path: Path | None = None, url: str | None = None) -> None:
        path = path or document_path(self.raw_root, sid)
        self.documents[sid].append({"url": url or DOCUMENTS[sid], "relative_path": str(path.relative_to(self.raw_root)), "sha256": sha256_for_file(path), "size_bytes": path.stat().st_size})
        if sid in HTML_RECHECK_SOURCES and path.suffix==".html":
            self.documents[sid][-1]["semantic_document_sha256"]=hashlib.sha256(html_text(path.read_text()).encode()).hexdigest()

    def fact(self, sid: str, metric: str, value: float, unit: str, page: str | int, *, entity_id: str = "", entity_name: str = "", entity_type: str = "agency", agency: str = "MoRTH", state: str = "All India", road_class: str = "National Highway", start: str = "", end: str = "", basis: str = "fiscal_year", estimate: str = "actual", statement: str = "agency", asof: str | None = None, published: str = "", disclosure_as_of: str = "", estimate_vintage: str = "", reported_period: str = "", evidence: str = "official_measured", eligible: bool = True, original_unit: str | None = None, notes: str = "", document_index: int = 0, **extra: Any) -> None:
        original_unit = original_unit or unit
        original_value = float(value)
        if unit == "INR crore":
            value *= {"INR crore": 1, "INR lakh": .01, "INR million": .1, "INR thousand": .0001}[original_unit]
        elif unit == "transactions":
            value *= {"transactions": 1, "million transactions": 1_000_000}[original_unit]
        doc = self.documents[sid][document_index]
        row = {column: "" for column in FACT_COLUMNS}
        row.update(entity_id=entity_id or identifier(agency), entity_name=entity_name or agency, entity_type=entity_type, agency=agency, state=state, road_class=road_class, metric=metric, value=float(value), unit=unit, original_value=original_value, original_unit=original_unit, period_start=start, period_end=end, period_basis=basis, estimate_type=estimate, statement_basis=statement, data_as_of=end if asof is None else asof, published_at=published, disclosure_as_of=disclosure_as_of, estimate_vintage=estimate_vintage, reported_period=reported_period, source_id=sid, citation_url=doc["url"], table_page=str(page), evidence_class=evidence, analytical_eligible=eligible, source_document_sha256=doc["sha256"], notes=notes, entity=entity_name or agency, year=int((end or asof or RESEARCH_CUTOFF)[:4]), metric_name=metric, metric_value=float(value), metric_category="issuer_disclosed" if sid.startswith("nhit_") else "official_measured", source_type="issuer" if sid.startswith("nhit_") else "official", comparison_group=f"{statement}|{road_class}")
        row.update(extra)
        row["analytical_eligible"] = bool(eligible and evidence not in {"target","valuation_estimate"})
        if agency=="NHIT":
            row["financing_entity_id"]="nhit"
            row["asset_owner_id"]="nhai"
            if entity_id.startswith(("nwppl","neppl","nsppl")):
                row["concessionaire_id"]=entity_id.split("_")[0]
        elif agency=="NHIDCL":
            row["implementing_agency_id"]="nhidcl"
        self.rows[sid].append(row)

    def budget(self) -> None:
        sid = "union_budget_morth_demand86"
        # Select total columns, never add transfers/recoveries to net expenditure.
        definitions = {1: [("Gross", "budget_gross_inr_crore", "MoRTH"), ("Recoveries", "budget_recoveries_inr_crore", "MoRTH"), ("Net", "budget_net_inr_crore", "MoRTH"), ("3.01", "budget_nhai_investment_inr_crore", "NHAI"), ("4.01", "budget_roads_wing_works_inr_crore", "MoRTH")], 2: [("4.04", "budget_state_crif_schemes_inr_crore", "MoRTH"), ("4.05", "budget_ut_crif_schemes_inr_crore", "MoRTH"), ("4.08", "budget_nh_maintenance_inr_crore", "MoRTH"), ("4.11", "budget_northeast_sardp_inr_crore", "MoRTH"), ("5.01", "budget_road_safety_inr_crore", "MoRTH")]}
        for page, items in definitions.items():
            # Layout text preserves each 12-number revenue/capital/total row.
            lines = self.reader(sid).pages[page - 1].extract_text(extraction_mode="layout").splitlines()
            for label, metric, agency in items:
                matches = [line for line in lines if re.match(r"\s*" + re.escape(label) + r"\s", line)]
                if label == "Net":
                    matches = matches[:1]
                assert len(matches) == 1, (label, matches)
                numeric = re.findall(r"(?<![A-Za-z0-9.])(-?\d[\d,]*\.\d{2}|\.\.\.)(?![\d.])", matches[0])
                if re.fullmatch(r"\d\.\d{2}", label):
                    numeric = numeric[1:]
                assert len(numeric) == 12, (label, numeric)
                for j, (year, estimate) in enumerate([(2024, "actual"), (2025, "BE"), (2025, "RE"), (2026, "BE")]):
                    start, end = fiscal_period(year)
                    # The current edition establishes its RE/current-BE vintage,
                    # not the original vintage of the reproduced prior BE column.
                    vintage = "2026-02-01" if j in (2, 3) else ""
                    self.fact(sid, metric, number(numeric[j * 3 + 2]), "INR crore", f"PDF p{page}; Demand 86 row {label}, Total column", agency=agency, start=start, end=end, estimate=estimate, asof=end if estimate == "actual" else vintage, published="2026-02-01", disclosure_as_of="2026-02-01", estimate_vintage=vintage, reported_period=f"{year}-{str(year+1)[2:]}", eligible=estimate == "actual" or bool(vintage), statement="Union Budget Demand 86", notes="BE and RE are estimates; inter-fund transfers are not additive to net expenditure. Actual observation cutoff is the fiscal year end. Current-edition RE/FY26-27BE vintage is1February2026; the reproduced FY25-26BE original vintage is undisclosed here and excluded from arithmetic.")
        sid = "union_budget_highway_outcomes"
        tables = [(238, "target_nh_construction_km", 10000, "km"), (238, "target_northeast_nh_construction_km", 1200, "km"), (238, "target_tribal_nh_construction_km", 400, "km"), (238, "target_operational_highspeed_corridor_km", 6000, "km"), (238, "target_private_investment_inr_crore", 30000, "INR crore"), (238, "target_ppp_awarded_length_share_percent", 30, "percent"), (238, "target_asset_monetisation_inr_crore", 30000, "INR crore"), (239, "target_blackspots_removed_count", 1000, "count"), (239, "target_wayside_amenities_awarded_count", 80, "count"), (239, "target_wayside_amenities_operational_count", 150, "count"), (239, "target_mlff_toll_coverage_km", 1200, "km"), (239, "target_toll_wait_seconds", 40, "seconds")]
        for page, metric, value, unit in tables:
            assert f"{value:,}" in self.text(sid, page) or str(value) in self.text(sid, page)
            self.fact(sid, metric, value, unit, f"PDF p{page}; printed p{page - 8}, Road Wing indicators", start="2026-04-01", end="2027-03-31", estimate="target", evidence="target", statement="Output Outcome Monitoring Framework", asof="2026-02-01", published="2026-02-01", notes="FY2026-27 target; does not measure achievement.")

    def nhit(self) -> None:
        sid = "nhit_quarterly_operations_finance"
        rows = [("revenue_operations_inr_crore", [1023, 1312], "INR crore"), ("other_income_inr_crore", [9, 11], "INR crore"), ("ebitda_inr_crore", [843, 1048], "INR crore"), ("finance_charges_inr_crore", [454, 480], "INR crore"), ("pat_inr_crore", [121, 235], "INR crore"), ("debt_outstanding_inr_crore", [21813, 25252], "INR crore"), ("dscr_ratio", [2.11, 2.49], "ratio"), ("debt_total_assets_ratio", [.48, .49], "ratio"), ("distribution_inr_crore", [578, 682], "INR crore"), ("distribution_per_unit_inr", [2.984, 3.187], "INR/unit"), ("units_outstanding_crore_units", [193.68, 213.86], "crore units")]
        text = self.text(sid, 12).replace(",", "")
        for metric, values, unit in rows:
            for year, value in zip([2025, 2026], values):
                assert str(value) in text, (metric, value)
                self.fact(sid, metric, value, unit, "PDF p12; printed slide 11, Financial Performance (Consolidated)", agency="NHIT", entity_id="nhit", entity_type="InvIT", start=f"{year}-04-01", end=f"{year}-06-30", basis="fiscal_quarter", asof=f"{year}-06-30", disclosure_as_of="2026-06-30", published="2026-08-31", statement="consolidated", evidence="issuer_disclosure", notes="Q1FY27 includes Round 5 from 1 April 2026; issuer presentation values are rounded. Historical comparator cutoff remains its own quarter end, distinct from the June2026 disclosure context.")
        for spv, name, values in [("nwppl_r1_r2", "NWPPL R1/R2", [266,299]), ("neppl", "NEPPL R3", [363,417]), ("nsppl", "NSPPL R4", [394,468]), ("nwppl_r5", "NWPPL R5", [None,128])]:
            for year, value in zip([2025,2026], values):
                if value is not None:
                    self.fact(sid, "revenue_operations_inr_crore", value, "INR crore", "PDF p12; printed slide 11", agency="NHIT", entity_id=spv, entity_name=name, entity_type="SPV_round", start=f"{year}-04-01", end=f"{year}-06-30", basis="fiscal_quarter", asof=f"{year}-06-30", disclosure_as_of="2026-06-30", published="2026-08-31", statement="SPV_round", evidence="issuer_disclosure")
        charts = [
            (9,"nwppl", "AP AS KK BK CK BM AB SJ".split(),[35208,38682,45386,32454,12868,24595,31103,18107],[34011,38963,50157,37466,14850,27507,36330,18458],[25,18,58,24,31,59,26,24],[24,18,67,28,38,68,31,25]),
            (10,"neppl","RKJL LK OB HHC ChK ASP".split(),[20752,29858,31302,26849,26072,20477],[22763,33855,31966,30264,29988,22523],[102,66,32,90,23,49],[123,77,33,104,27,54]),
            (11,"nsppl","RSB MH AN VIZBY GDK CM BS GM".split(),[24744,37801,32973,27306,30295,29716,24253,52842],[28344,51794,36294,30924,33962,33911,25967,50985],[61,58,65,25,46,60,59,45],[69,69,73,29,52,69,64,45]),
        ]
        for page, spv, codes, t0, t1, r0, r1 in charts:
            text = self.text(sid, page).replace(",", "")
            for i, code in enumerate(codes):
                for year, traffic, revenue in [(2025,t0[i],r0[i]),(2026,t1[i],r1[i])]:
                    assert str(traffic) in text and str(revenue) in text
                    statement = "IHMCL_ETC_only" if spv == "nsppl" and year == 2025 else "ETC_non_ETC_including_exempt"
                    for metric, value, unit in [("traffic_pcu",traffic,"PCU"),("toll_revenue_inr_crore",revenue,"INR crore")]:
                        self.fact(sid, metric, value, unit, f"PDF p{page}; printed slide {page-1}, asset chart {code}", agency="NHIT", entity_id=f"{spv}_{code.lower()}", entity_name=f"{spv.upper()} / {code}", entity_type="toll_asset_group", asset_id=f"{spv}_{code.lower()}", start=f"{year}-04-01", end=f"{year}-06-30", basis="fiscal_quarter", asof=f"{year}-06-30", disclosure_as_of="2026-06-30", published="2026-08-31", statement=statement, evidence="issuer_disclosure", notes="NSPPL FY26 traffic uses ETC only; FY27 includes non-ETC/exempt: these bases cannot support a comparable YoY calculation. FY27 toll revenue includes annual pass compensation.")
        sid = "nhit_quarterly_financial_filings"
        for page, metric, value, unit, evidence in [(1,"distribution_per_unit_inr",3.187,"INR/unit","issuer_disclosure"),(1,"distribution_interest_per_unit_inr",3.179,"INR/unit","issuer_disclosure"),(1,"distribution_other_income_per_unit_inr",.008,"INR/unit","issuer_disclosure"),(2,"enterprise_value_inr_crore",58245,"INR crore","valuation_estimate"),(2,"nav_pre_distribution_per_unit_inr",159.35,"INR/unit","valuation_estimate"),(2,"nav_post_distribution_per_unit_inr",156.16,"INR/unit","valuation_estimate")]:
            assert str(value) in self.text(sid,page).replace(",", "")
            self.fact(sid,metric,value,unit,f"PDF p{page}; Board outcome paragraphs 2-4",agency="NHIT",entity_id="nhit",entity_type="InvIT",start="2026-04-01",end="2026-06-30",basis="fiscal_quarter",asof="2026-06-30",published="2026-08-07",statement="board_approved_distribution" if page==1 else "independent_valuation",evidence=evidence)
        self.notes[sid] = "Board text validated. Annexure I scanned statement pages remain quarantined; no unvalidated OCR accounting lines entered."
        sid = "nhit_asset_valuation_assumptions"
        for round_id, spv, count, fee, effective in [(1,"NWPPL",5,74514,"2021-12-16"),(2,"NWPPL",3,28497,"2022-10-29"),(3,"NEPPL",7,156999,"2024-04-01"),(4,"NSPPL",11,177379,"2025-04-01"),(5,"NWPPL",2,63669,"2026-04-01")]:
            assert f"{fee:,}" in self.text(sid,5)
            for metric,value,unit,original in [("invit_concession_value_inr_crore",fee,"INR crore","INR million"),("concession_asset_count",count,"count","count")]:
                self.fact(sid,metric,value,unit,"PDF p5; printed p4, Executive Summary concession table",entity_id=f"nhit_round_{round_id}",entity_name=f"NHIT Round {round_id} / {spv}",entity_type="InvIT_round",agency="NHIT",start=effective,end=effective,basis="transaction",asof=effective,disclosure_as_of="2026-06-30",published="2026-08-07",statement="management_concession_fee",original_unit=original,evidence="issuer_disclosure",notes="Original concession fee INR million; related-party concession transaction, distinct from valuation enterprise value. Observation is the stated effective date; June2026 is the later reporting context.")
        for value,metric,unit in [(2653,"portfolio_length_km","km"),(13315,"portfolio_lane_length_km","lane-km"),(13,"portfolio_state_count","count")]:
            assert str(value) in self.text(sid,6).replace(",", "")
            self.fact(sid,metric,value,unit,"PDF p6; printed p5",agency="NHIT",entity_id="nhit",entity_type="InvIT",end="2026-06-30",basis="stock",asof="2026-06-30",published="2026-08-07",statement="valuation_portfolio",evidence="issuer_disclosure",notes="Approximate portfolio route length; issuer presentation separately reports 2,655km. Source definitions preserved.")
        for spv,wacc in [("nwppl_r1_r2",9.8),("neppl",9.85),("nsppl",9.75),("nwppl_r5",9.75)]:
            assert f"{wacc:.2f}" in self.text(sid,73)
            self.fact(sid,"wacc_percent",wacc,"percent","PDF p73; printed p72, WACC computation",agency="NHIT",entity_id=spv,entity_name=spv.upper(),entity_type="SPV_round",end="2026-06-30",basis="valuation_date",asof="2026-06-30",published="2026-08-07",statement="independent_valuation",evidence="valuation_estimate",notes="Valuer assumes50:50 debt/equity; WACC is a model assumption, not measured borrowing yield.")
        sid="nhit_annual_report"
        # Audited statements are text-native here, unlike quarterly Annexure I.
        lines={161:self.text(sid,161).splitlines(),162:self.text(sid,162).splitlines()}
        definitions={161:[("TOTAL ASSETS","total_assets_inr_crore"),("Total Equity","equity_inr_crore"),("Total liabilities","total_liabilities_inr_crore"),("(i) Borrowings 20","noncurrent_borrowings_inr_crore"),("(i) Borrowings 24","current_borrowings_inr_crore"),("(ii) Cash and Cash Equivalents","cash_equivalents_inr_crore")],162:[("Revenue from Operations","revenue_operations_inr_crore"),("TOTAL INCOME","total_income_inr_crore"),("Operating Expenses","operating_expenses_inr_crore"),("Finance Cost","finance_charges_inr_crore"),("Depreciation & Amortization Expense","depreciation_amortisation_inr_crore"),("TOTAL EXPENSES","total_expenses_inr_crore"),("Profit before Tax","profit_before_tax_inr_crore"),("Profit after tax","pat_inr_crore")]}
        for page,items in definitions.items():
            assert "All amounts in ₹ lakh" in self.text(sid,page)
            for label,metric in items:
                line=next(line for line in lines[page] if line.strip().startswith(label))
                values=re.findall(r"(?<![\d.])[\d,]+\.\d{2}(?![\d.])",line)
                assert len(values)==2,(page,label,values)
                for year,value in zip([2025,2024],values):
                    start,end=fiscal_period(year)
                    self.fact(sid,metric,number(value),"INR crore",f"PDF p{page}; printed p{page*2-2}/{page*2-1}; {label}",agency="NHIT",entity_id="nhit",entity_type="InvIT",start=start,end=end,basis="stock" if page==161 else "fiscal_year",asof=end,statement="consolidated_audited",original_unit="INR lakh",evidence="issuer_disclosure",notes="Audited statements signed13May2026; report publication day undisclosed. Current/noncurrent borrowings are separate from contractual undiscounted maturity buckets.")
        for spv,toll,other in [("nwppl",1097.68,11.93),("neppl",1513.09,8.11),("nsppl",1663.30,9.08)]:
            for metric,value in [("toll_revenue_inr_crore",toll),("other_income_inr_crore",other)]:
                assert f"{value:,.2f}" in self.text(sid,17)
                self.fact(sid,metric,value,"INR crore","PDF p17; printed p32/33, SPV overview",agency="NHIT",entity_id=spv,entity_name=spv.upper(),entity_type="SPV",start="2025-04-01",end="2026-03-31",asof="2026-03-31",statement="SPV_overview_approximate",evidence="issuer_disclosure",notes="Approximate issuer SPV revenue overview; R5 concession appointed1April2026.")
        # Carrying amount and maturity profiles are separate concepts. All three
        # debt instruments are retained individually; 'all financial liabilities'
        # also includes leases/trade payables and is not labelled as debt.
        text=self.text(sid,205)
        assert "contractual undiscounted payments" in text and "All amounts in ₹ lakh" in text
        # A printed dash is nonnumeric. Without an explicit nil definition, it
        # cannot establish a zero maturity amount or supply an original value.
        instruments=[("term_loan",[2255334.08,23349.70,55684.90,2176299.48],[1918534.78,19899.70,44656.40,1853978.68]),("ncd",[148754.05,None,None,148754.05],[148694.23,None,None,148694.23]),("zero_coupon_bond",[99827.01,None,None,99827.01],[99820.22,None,None,99820.22])]
        for instrument,fy26,fy25 in instruments:
            for year,values in [(2025,fy26),(2024,fy25)]:
                for metric,value in zip(["debt_carrying_amount_inr_crore","debt_maturity_lt1yr_inr_crore","debt_maturity_1to3yr_inr_crore","debt_maturity_gt3yr_inr_crore"],values):
                    if value is None:
                        continue
                    assert f"{value:.2f}" in text.replace(",", ""),(instrument,value)
                    start,end=fiscal_period(year)
                    self.fact(sid,metric,value,"INR crore","PDF p205; printed p408/409, contractual maturity profile",agency="NHIT",entity_id=f"nhit_{instrument}",entity_name=f"NHIT {instrument.replace('_',' ')}",entity_type="debt_instrument",start=start,end=end,basis="stock",asof=end,statement="contractual_undiscounted_maturity" if "maturity" in metric else "consolidated_audited_carrying_amount",original_unit="INR lakh",evidence="issuer_disclosure",notes="Source labels maturity contractual undiscounted payments; keep maturity buckets distinct from carrying amounts and borrowing balance. Eight NCD/zero-coupon bond <1year and1-3year source dashes are omitted as nonnumeric; no zero is inferred.")
        text=self.text(sid,245)
        for metric,values in [("debt_carrying_amount_inr_crore",[25039.15,21670.49]),("additional_borrowings_inr_crore",[3571.01,11192.05]),("debt_repayments_inr_crore",[200.24,1245.58]),("debt_maturity_lt1yr_inr_crore",[233.50,199.00]),("debt_maturity_1to3yr_inr_crore",[556.85,446.56]),("debt_maturity_gt3yr_inr_crore",[24248.80,21024.93])]:
            for year,value in zip([2025,2024],values):
                assert f"{value:.2f}" in text.replace(",", "")
                start,end=fiscal_period(year)
                self.fact(sid,metric,value,"INR crore","PDF p245; printed p488/489, external borrowing/debt maturity overview",agency="NHIT",entity_id="nhit_debt_all",entity_name="NHIT all external borrowings",entity_type="debt_aggregate",start=start,end=end,basis="stock" if "carrying" in metric or "maturity" in metric else "fiscal_year",asof=end,statement="contractual_undiscounted_maturity" if "maturity" in metric else "external_borrowings_overview",evidence="issuer_disclosure",notes="All debt instruments, excluding lease/trade/payable financial liabilities. Overview is rounded INRcrore; repayment amounts shown as outflow magnitude, not signed debt balance.")
        self.notes[sid]="Audited consolidated balance sheet/P&L and financial-risk debt maturity note extracted and unit-checked. Eight NCD/zero-coupon bond <1year and1-3year source dashes on physicalp205 are nonnumeric and omitted, not converted to zero. Numeric debt-overview totals remain separately sourced. Issuer SPV operating overview is separately labelled approximate; publication day not asserted."

    def parliament_and_audit(self) -> None:
        sid="parliament_nhai_debt_tot_invit"
        assert "2,37,247.95" in self.text(sid,2)
        self.fact(sid,"debt_outstanding_inr_crore",237247.95,"INR crore","PDF p2; answer(a), latest quarter31December2025",agency="NHAI",entity_id="nhai",end="2025-12-31",basis="stock",asof="2025-12-31",disclosure_as_of="2026-02-05",published="2026-02-05",statement="parliament_reply")
        bundles=[("1",2018,681,9682,"Andhra Pradesh; Odisha; Gujarat"),("3",2020,566,5011,"Uttar Pradesh; Bihar; Jharkhand; Tamil Nadu"),("5A1",2021,54,1011,"Gujarat"),("5A2",2022,106,1251,"Gujarat"),("7",2022,135,6267,"Eastern Peripheral Expressway"),("9",2022,73,3144,"Uttar Pradesh"),("11",2023,84,2156,"Uttar Pradesh"),("12",2023,316,4428,"Madhya Pradesh"),("13",2023,108,1683,"Madhya Pradesh; Rajasthan"),("14",2023,189,7701,"Uttar Pradesh; Delhi; Odisha"),("16",2024,252,6661,"Hyderabad-Nagpur"),("17",2025,366,9270,"Uttar Pradesh")]
        for bundle,year,length,fee,state in bundles:
            assert str(fee) in self.text(sid,3)
            start,end=fiscal_period(year)
            current_year = year == 2025
            if current_year:
                end = "2026-02-05"
            for metric,value,unit in [("tot_portfolio_length_km",length,"km"),("tot_concession_value_inr_crore",fee,"INR crore")]:
                self.fact(sid,metric,value,unit,f"PDF p3/4; Annexure A, TOT-{bundle}",agency="NHAI",entity_id="tot_"+bundle.lower(),entity_name="TOT Bundle "+bundle,entity_type="TOT_bundle",state=state,start=start,end=end,basis="fiscal_year_to_date" if current_year else "fiscal_year",estimate="YTD" if current_year else "actual",asof=end,disclosure_as_of="2026-02-05",published="2026-02-05",reported_period=f"{year}-{str(year+1)[2:]}",statement="receipts_deposited_CFI",notes="Concession proceeds transferred to CFI; not direct NHAI toll revenue or a measure of debt principal retired. Fiscal year records receipts, not necessarily concession execution date. Closed-year observations retain their year-end cutoff; FY25-26 is only observed through the5February2026 answer, not a full-year result.")
        for round_id,year,length,fee in [(1,2021,389,7350),(2,2022,246,2850),(3,2023,889,15700),(4,2024,754,17738)]:
            assert f"{fee:,}" in self.text(sid,4)
            start,end=fiscal_period(year)
            # The Round4 footnote moves the receipt to FY25-26. Preserve the
            # table's FY24-25 label separately, rather than using it as receipt year.
            if round_id == 4:
                start,end = "2025-04-01","2026-02-05"
            for metric,value,unit in [("invit_portfolio_length_km",length,"km"),("invit_concession_value_inr_crore",fee,"INR crore")]:
                self.fact(sid,metric,value,unit,f"PDF p4; Annexure B, Round {round_id}",agency="NHAI",entity_id=f"nhit_round_{round_id}",entity_name=f"InvIT Round {round_id}",entity_type="InvIT_round",start=start,end=end,basis="fiscal_year_to_date" if round_id == 4 else "fiscal_year",estimate="YTD" if round_id == 4 else "actual",asof=end,disclosure_as_of="2026-02-05",published="2026-02-05",reported_period=f"{year}-{str(year+1)[2:]}",statement="parliament_receipts",notes="Round4 FY24-25 shown in table; footnote says receipts deposited CFI FY25-26. Its receipt observation ends at the5February2026 answer, while reported_period preserves the table's round-year label. Rounded Parliamentary fees differ from issuer concession fees; never add both assertions. Portfolio length describes the receipt bundle, not length newly constructed in that fiscal year.")
        sid="cag_bharatmala_performance_audit"
        text=self.text(sid,11)+self.text(sid,12)
        for metric,value,unit in [("programme_approved_length_km",34800,"km"),("programme_approved_outlay_inr_crore",535000,"INR crore"),("programme_awarded_length_km",26316,"km"),("programme_sanctioned_cost_inr_crore",846588,"INR crore"),("programme_completed_length_km",13499,"km"),("audit_sample_project_count",66,"count")]:
            assert str(value) in text.replace(",", "")
            self.fact(sid,metric,value,unit,"PDF p11/12; printed iii/iv, Executive Summary",agency="MoRTH",entity_id="bharatmala_phase1",entity_name="Bharatmala Phase I",entity_type="programme",end="2023-03-31",basis="programme_cumulative",asof="2023-03-31",statement="CAG_performance_audit",evidence="audit_finding",notes="Report19of2023; main audit FY17-18 throughFY20-21, programme totals updated31March2023. Sample66projects is not a national project risk denominator.")

    def monetisation(self) -> None:
        sid="nhai_monetisation_transactions"
        html=document_path(self.raw_root,sid).read_text()
        text=html_text(html)
        self.notes[sid]="PIB release30March2026 reports FY25-26 progress before year end; target30000crore is not actual."
        for metric,value,unit,entity,entity_type in [("monetisation_realised_inr_crore",28307,"INR crore","nhai","agency"),("monetisation_target_inr_crore",30000,"INR crore","nhai","agency"),("invit_concession_value_inr_crore",6366.98,"INR crore","nhit_round_5","InvIT_round"),("invit_portfolio_length_km",310,"km","nhit_round_5","InvIT_round"),("tot_concession_value_inr_crore",3087,"INR crore","tot_18","TOT_bundle"),("tot_portfolio_length_km",74.5,"km","tot_18","TOT_bundle")]:
            assert f"{value:,}" in text or str(value) in text.replace(",", ""),(metric,value)
            self.fact(sid,metric,value,unit,"PIB PRID2247011;30March2026 body paragraphs",agency="NHAI",entity_id=entity,entity_name="NHAI" if entity=="nhai" else entity.upper().replace("_"," "),entity_type=entity_type,start="2025-04-01",end="2026-03-30",basis="fiscal_year_to_date",estimate="target" if "target" in metric else "YTD",asof="2026-03-30",published="2026-03-30",statement="press_release_progress",evidence="target" if "target" in metric else "official_measured",notes="Release is30March, not full financial year actual; InvIT5 related-party concession value differs slightly from later issuer disclosures. Target and realised amounts are separate.")

    def upeida(self) -> None:
        sid="upeida_expressway_projects"
        if not document_path(self.raw_root,sid).exists():
            return
        text=html_text(document_path(self.raw_root,sid).read_text())
        for group,grant in enumerate([1746,1720,2177,2099],1):
            assert str(grant) in text.replace(",", "")
            self.fact(sid,"government_grant_inr_crore",grant,"INR crore",f"Ganga Expressway page; project group {group} grant table",agency="UPEIDA",entity_id=f"upeida_ganga_group_{group}",entity_name=f"Ganga Expressway Group {group}",entity_type="project_group",state="Uttar Pradesh",road_class="Expressway",basis="project_disclosure",statement="award_grant_table",eligible=False,notes="Page publication and financial observation date undisclosed; award dated16December2021; grant amounts are award disclosures, not expenditure or completion measures.",implementing_agency_id="upeida")
        assert "593.947" in text
        self.fact(sid,"project_length_km",593.947,"km","Ganga Expressway page; route length",agency="UPEIDA",entity_id="upeida_ganga",entity_name="Ganga Expressway",entity_type="project",state="Uttar Pradesh",road_class="Expressway",basis="project_disclosure",statement="project_route",eligible=False,notes="Page observation date undisclosed; length only, no inference of opening/completion from progress link.",implementing_agency_id="upeida")
        agra=document_path(self.raw_root,sid).parent/"agra-lucknow.html"
        if agra.exists():
            self.pin(sid,agra,"https://upeida.up.gov.in/en/page/agra-lucknow-expressway")
            text=html_text(agra.read_text())
            for metric,value,unit in [("project_length_km",302.222,"km"),("project_cost_excluding_land_inr_crore",11526.73,"INR crore"),("lane_count",6,"count")]:
                assert str(value) in text.replace(",", "")
                self.fact(sid,metric,value,unit,"Agra-Lucknow Expressway page; project summary",agency="UPEIDA",entity_id="upeida_agra_lucknow",entity_name="Agra-Lucknow Expressway",entity_type="project",state="Uttar Pradesh",road_class="Expressway",basis="project_disclosure",statement="project_cost_excluding_land" if "cost" in metric else "project_route",eligible=False,document_index=1,notes="Cost EXCLUDES land. Expressway operational21November2016, but cost observation/publication dates not disclosed; facts retained for context and excluded from dated analytics.",implementing_agency_id="upeida")
        self.notes[sid]="TLS-verified macOS curl retrieved historical project/grant HTML; source publication/observation dates undisclosed, figures quarantined from dated rankings. Current Ganga progress PDF URL unavailable; no progress actuals claimed."

    def nhidcl(self) -> None:
        import pdfplumber
        sid = "nhidcl_monthly_project_progress"
        with pdfplumber.open(document_path(self.raw_root,sid)) as pdf:
            table = pdf.pages[1].extract_tables()[0]
        assert len(table)==411 and len(table[0])==20
        ids = set()
        for row in table[1:]:
            sequence,state,pmu,upn,stage,name,nh,length,constructed,tpc,awarded,award,appointed,physical,financial,scheduled,likely,actual,contractor,email = row
            upn = re.sub(r"\s+", "", upn)
            assert upn not in ids, upn
            ids.add(upn)
            state,name,stage = [re.sub(r"\s+", " ",value).strip() for value in (state,name,stage)]
            original_state=state
            state={"Andaman & Nicobar":"Andaman and Nicobar Islands","Assam (MMLP)":"Assam","Jammu & Kashmir (RO- Jammu)":"Jammu & Kashmir","Jammu & Kashmir (RO- Srinagar)":"Jammu & Kashmir"}.get(state,state)
            attrs = dict(agency="NHIDCL",entity_id=upn,entity_name=name,entity_type="project",state=state,road_class="multimodal_logistics" if "MMLP" in original_state else "National Highway",end="2026-08-31",basis="project_snapshot",asof="2026-08-31",statement="PMP_Data_Lake",project_name=name,asset_id=upn,nh_number=re.sub(r"\s+","",nh),project_stage=stage,scheduled_completion_date=scheduled,likely_completion_date=likely,actual_completion_date=actual,contractor_name=re.sub(r"\s+"," ",contractor or ""),notes=f"Original source State/UT/RO label: {original_state}. Detailed table includes under-termination and MMLP projects; summary ongoing scope excludes under termination. Reported constructed length may differ from contractual route length. Contractor is not assumed to be operator/concessionaire; undisclosed roles are blank.")
            for metric,value,unit in [("project_length_km",length,"km"),("constructed_length_km",constructed,"km"),("sanctioned_cost_inr_crore",tpc,"INR crore"),("awarded_cost_inr_crore",awarded,"INR crore"),("physical_progress_percent",physical,"percent"),("financial_progress_percent",financial,"percent")]:
                if re.fullmatch(r"[\d,.]+",value or ""):
                    self.fact(sid,metric,number(value),unit,f"PDF p2; project row {sequence}; UPN {upn}",**attrs)
        self.notes[sid] = "410 detail projects; do not sum to page1 summary: summary has a narrower stage/agency classification. Emails excluded."

    def rbi(self) -> None:
        import pdfplumber
        sid = "rbi_state_road_finances"
        with pdfplumber.open(document_path(self.raw_root,sid)) as pdf:
            for p in range(221,323):
                text = pdf.pages[p].extract_text()
                if "Roads and Bridges" not in text:
                    continue
                if p < 269:
                    metric, statement = "roads_bridges_revenue_expenditure_inr_crore", "revenue_account"
                else:
                    metric, statement = "roads_bridges_capital_outlay_inr_crore", "capital_outlay"
                tables = [table for table in pdf.pages[p].extract_tables() if len(table[0]) in (5,9) and table[0][0]=="Item"]
                assert len(tables)==1, p+1
                table = tables[0]
                # These tables store the data block as newline-separated columns;
                # Roads+Bridges is the second numeric row in the relevant block.
                block = next(row for row in table if "Roads and Bridges" in (row[0] or ""))
                labels = [x for x in block[0].splitlines() if x.strip()]
                assert "Roads and Bridges" in labels[1], (p+1,labels[:3])
                for offset in range(1,len(table[0]),4):
                    state = table[0][offset]
                    if not state or not block[offset]:
                        continue
                    state = state.title().replace("And Uts","and UTs")
                    state = {"Tamilnadu":"Tamil Nadu", "Uttarpradesh":"Uttar Pradesh", "Westbengal":"West Bengal", "Jammu And Kashmir":"Jammu & Kashmir"}.get(state,state)
                    for i,(year,estimate) in enumerate([(2023,"actual"),(2024,"BE"),(2024,"RE"),(2025,"BE")]):
                        assert block[offset+i] is not None, (p+1,state,i,block)
                        values = block[offset+i].splitlines()
                        raw = values[1].strip()
                        if raw in {"–","-","..."}:
                            continue
                        assert re.fullmatch(r"[\d,.]+",raw),(p+1,state,raw)
                        start,end = fiscal_period(year)
                        self.fact(sid,metric,number(raw),"INR crore",f"PDF p{p+1}; printed p{p-12}; {'Appendix II' if p<269 else 'Appendix IV'} Roads and Bridges",agency="State governments",entity_id="state_"+identifier(state),entity_name=state,entity_type="state_aggregate",state=state,road_class="roads_and_bridges_all_classes",start=start,end=end,estimate=estimate,asof=end if estimate == "actual" else "",disclosure_as_of="2026-01-23",published="2026-01-23",reported_period=f"{year}-{str(year+1)[2:]}",eligible=estimate == "actual",statement=statement,original_unit="INR lakh",notes="Functional Roads and Bridges expenditure includes all road classes; it is not a State Highway-only budget. BE/RE retained separately. Revenue account and capital outlay exclude loans/repayments. Accounts observations end31March2024. State-specific BE/RE original vintage is undisclosed;23January2026 is publication, not estimate observation, and unknown-vintage estimates are excluded from arithmetic.")
        assert len(self.rows[sid]) == 252, len(self.rows[sid])
        from pipelines.rbi_state_finance_tables import extract_state_liabilities
        for record in extract_state_liabilities(document_path(self.raw_root,sid)):
            state=record["state"].replace("Jammu and Kashmir","Jammu & Kashmir")
            entity_id="state_government_"+identifier(state)
            actual = record["estimate_type"] == "actual"
            self.fact(sid,record["metric"],record["value"],record["unit"],record["table_page"],agency="State governments",entity_id=entity_id,entity_name=state+" government",entity_type="state_government",state=state,road_class="All sectors",end=record["period_end"],basis="balance_sheet_snapshot",estimate=record["estimate_type"],asof=record["period_end"] if actual else "",disclosure_as_of=record["disclosure_as_of"],published=record["published_at"],reported_period=record["reported_period"],eligible=actual,statement="state_government_all_sectors",financing_entity_id=entity_id,notes=record["notes"]+" State-specific RE/BE estimate vintage is undisclosed; publication is preserved separately and never becomes an observation cutoff.")
        assert len(self.rows[sid])==1302,len(self.rows[sid])

    def brs(self) -> None:
        import pdfplumber
        sid = "morth_state_highway_network"
        with pdfplumber.open(document_path(self.raw_root,sid)) as pdf:
            tables = pdf.pages[163].extract_tables()
        # The file contains duplicate spreads on p164/165; only one is read.
        table = tables[1]
        assert len(table)==41 and "Un-surfaced" in table[0][-1]
        for row in table[3:-1]:
            raw = row[1].replace("\n", " ")
            state = re.sub(r"[\s#*@]+$", "", raw).strip()
            marker = "**" if "**" in raw or "* *" in raw else "*" if "*" in raw else "#" if "#" in raw else "@" if "@" in raw else ""
            asof={"*":"2018-03-31","#":"2019-03-31","@":"2020-03-31","**":"2021-03-31","":"2022-03-31"}[marker]
            for metric,index in [("sh_network_length_km",2),("sh_surfaced_length_km",5)]:
                self.fact(sid,metric,number(row[index]),"km","PDF p164; Annexure 2.3.2 (duplicate spread p165 excluded)",agency="State PWDs",entity_id="state_"+identifier(state),entity_name=state,entity_type="state_aggregate",state=state,road_class="State Highway",end=asof,basis="stock",asof=asof,statement="BRS_Annexure_2_3_2",eligible=state!="Arunachal Pradesh",notes=f"Report headline 31 March 2022; state observation follows footnote {marker or 'none'}. Published SH total193740km differs from state sum193741km. Arunachal duplicated AP value13500km retained but quarantined pending correction; surface components contain rounding/internal discrepancies.")
        self.fact(sid,"sh_network_length_km",193740,"km","PDF p164; Annexure 2.3.2 published Total",agency="State PWDs",entity_id="brs_published_total",entity_name="BRS published SH total",entity_type="published_total",road_class="State Highway",end="2022-03-31",basis="stock",asof="2022-03-31",statement="BRS_Annexure_2_3_2",eligible=False,notes="Published total193740km; state rows sum193741km; aggregate quarantined and not reconciled artificially.")
        self.notes[sid]="SH network all states/UTs, older per-state footnotes preserved; Arunachal and inconsistent published total quarantined. NH stock is separately covered by existing MoRTH source; no duplicate NH series created."

    def adb_project_accounts(self) -> None:
        """Visually checked cells from the pinned scanned final project accounts.

        Embedded OCR misreads 569785 as569795 and674809 as674909. Transcription
        is valid only for these exact publisher PDF bytes and physical pages;
        a new report/hash requires another visual table review, never guessed OCR.
        """
        sid = "adb_state_road_projects"
        path = document_path(self.raw_root, sid)
        if not path.exists():
            return
        if sha256_for_file(path) != ADB_PROJECT_PDF_SHA256:
            raise ValueError("ADB scanned accounts changed; new visual extraction required")
        if len(self.reader(sid).pages) != 25:
            raise ValueError("ADB final project accounts page contract changed")
        if not self.documents[sid]:
            self.pin(sid)
        self.documents[sid][0].update(
            artifact_kind="publisher_pdf", checksum_scope="Exact downloaded publisher PDF bytes",
            extraction_method="Checksum-pinned visual transcription of scanned tables; embedded OCR is not numerical authority",
            publication_reference_url="https://www.adb.org/projects/documents/ind-52298-001-apfs-3",
        )
        self.rows[sid] = []
        periods = [("2024-04-01", "2025-05-19", "reporting_period"),
                   ("2023-04-01", "2024-03-31", "fiscal_year"),
                   ("2020-04-01", "2025-05-19", "project_cumulative")]
        scope = dict(entity_id="adb_52298_001", entity_name="Maharashtra State Road Improvement Project / ADB52298-001", entity_type="project",
                     agency="MPWD", state="Maharashtra", road_class="SH and MDR",
                     published="2025-09-05", evidence="borrower_audited_project_disclosure",
                     implementing_agency_id="maharashtra_public_works_department", financing_entity_id="adb",
                     project_name="Maharashtra State Road Improvement Project", source_type="issuer", metric_category="issuer_disclosed")
        limits = ("Cash-basis MPWD project accounts, Loan3911-IND; Government of India is borrower and ADB lender. "
                  "Project finances are not whole-state debt or an SPV balance sheet. Printed INR'000 normalized by0.0001 to INR crore. "
                  "Current period ends19May2025 rather than31March2025; cumulative flows overlap annual flows and must not be added. "
                  "Amounts in thousands are rounded; minor component discrepancies are preserved, not reconciled by invented values. ")
        # Current/prior/cumulative columns, physical Annexure1 p3-4. None means
        # source dash/absence; the helper deliberately creates no numerical fact.
        payments = [
            ("project_government_funds_received_inr_crore", [135800,1373033,17253814], 3, "Funds received from Government"),
            ("project_civil_works_expenditure_inr_crore", [460306,1215115,14934706], 3, "Civil Works"),
            ("project_utility_shifting_expenditure_inr_crore", [8652,16694,779156], 3, "Utility Shifting"),
            ("project_social_mitigation_expenditure_inr_crore", [None,7633,47250], 3, "Social Mitigation and gender aspects"),
            ("project_environmental_mitigation_expenditure_inr_crore", [None,None,15262], 3, "Environmental Mitigation"),
            ("project_authority_engineer_expenditure_inr_crore", [26988,28737,383223], 3, "Authority Engineer"),
            ("project_management_services_expenditure_inr_crore", [10000,None,36307], 3, "Project Management Services"),
            ("project_rnmp_expenditure_inr_crore", [62434,None,62434], 3, "Road Network Master Plan"),
            ("project_investment_expenditure_inr_crore", [568379,1268179,16258337], 3, "Subtotal(D) Investment Costs"),
            ("project_recurrent_management_expenditure_inr_crore", [1406,16225,66947], 3, "Project Management(MPWD), Subtotal(E)"),
            ("project_total_expenditure_inr_crore", [569785,1284404,16325284], 4, "Total Payments / Total Project Cost"),
        ]
        for metric, values, page, label in payments:
            for (start,end,basis),value in zip(periods,values):
                if value is not None:
                    self.fact(sid,metric,value,"INR crore",f"PDF p{page}; Annexure1; {label}",original_unit="INR thousand",start=start,end=end,basis=basis,asof=end,statement="project_cash_basis_receipts_payments",notes=limits+"ADB Loan receipts, maintenance, financing-charge and resettlement source dashes have no numerical facts.",**scope)
        for end,value in [("2025-05-19",928530),("2024-03-31",1362515)]:
            attrs = scope | dict(eligible=end!="2025-05-19")
            mismatch = " Note5 nine printed package deposits sum928394 versus reported total928530 INRthousand, an unreconciled136-thousand discrepancy; this reported total is excluded from measured arithmetic, without synthetic balancing." if end=="2025-05-19" else ""
            self.fact(sid,"project_cash_bank_balance_inr_crore",value,"INR crore","PDF p4; Annexure1; Closing bank Balance(C-H); Note5 p11",original_unit="INR thousand",end=end,basis="balance_sheet_snapshot",asof=end,statement="project_cash_basis_contractual_deposits",notes=limits+"Note5 identifies retention, maintenance, PartV and security deposits at PIUs; not unrestricted project cash."+mismatch,**attrs)
        # Annexure2 p5 current-period funding shares. Percentages and whole-project
        # totals elsewhere are not substituted for these paid expenditure amounts.
        shares = [
            ("project_adb_financed_civil_works_inr_crore",228543,"ADB Civil Works"),
            ("project_government_financed_civil_works_inr_crore",231762,"Government Civil Works"),
            ("project_government_financed_utility_shifting_inr_crore",8652,"Government Utility Shifting"),
            ("project_adb_financed_authority_engineer_inr_crore",7948,"ADB Authority Engineer"),
            ("project_government_financed_authority_engineer_inr_crore",19040,"Government Authority Engineer"),
            ("project_adb_financed_management_services_inr_crore",4250,"ADB Project Management Services"),
            ("project_government_financed_management_services_inr_crore",5750,"Government Project Management Services"),
            ("project_adb_financed_rnmp_inr_crore",52605,"ADB Road Network Master Plan"),
            ("project_government_financed_rnmp_inr_crore",9829,"Government Road Network Master Plan"),
            ("project_adb_financed_investment_inr_crore",293346,"ADB Subtotal(A)"),
            ("project_government_financed_investment_inr_crore",275033,"Government Subtotal(A)"),
            ("project_adb_financed_total_expenditure_inr_crore",293346,"ADB Total Cost(C=A+B)"),
            ("project_government_financed_total_expenditure_inr_crore",276439,"Government Total Cost(C=A+B)"),
        ]
        for metric,value,label in shares:
            self.fact(sid,metric,value,"INR crore",f"PDF p5; Annexure2; {label}; Actual Expenditure",original_unit="INR thousand",start=periods[0][0],end=periods[0][1],basis="reporting_period",asof=periods[0][1],statement="project_cash_basis_category_financier",notes=limits+"ADB financing allocation differs from disbursement of prior-year outstanding claims.",**scope)
        claims = [
            ("project_adb_eligible_expenditure_claimed_inr_crore",[674809,1459739,11556815],"Subtotal(A) / Total Eligible Expenditure Claimed(F)"),
            ("project_adb_disbursement_current_year_expenditure_inr_crore",[293346,982208,None],"Disbursement from current-year expenditure"),
            ("project_adb_disbursement_prior_year_outstanding_inr_crore",[381462,477531,None],"Disbursement from previous-year outstanding"),
            ("project_borrower_share_of_claims_inr_crore",[276439,340801,4768469],"Borrower share(E)"),
        ]
        for metric,values,label in claims:
            for (start,end,basis),value in zip(periods,values):
                if value is not None:
                    self.fact(sid,metric,value,"INR crore",f"PDF p6; Annexure3; {label}; reimbursement confirmed p13 Annexure5",original_unit="INR thousand",start=start,end=end,basis=basis,asof=end,statement="project_cash_basis_disbursement_claims",notes=limits+"Reimbursed/eligible claimed expenditure is not outstanding debt principal; grants and unclaimed-expenditure dashes are omitted.",**scope)
        packages = [
            (1,"Ratnagiri",1029105,96320,"NH66 to Kante Tulsani Devrukh Marleshwar SH174; Chafe-Ganpatipule MDR55",11),
            (2,"Ratnagiri",1399141,129213,"Dabhole Shiposhi Korle Vatul joining NH175; Hatiwale-Jaitapur SH170",11),
            (3,"Solapur",2001478,43664,"Barshi-Solapur SH204",11),
            (4,"Akluj",2203099,51351,"Korti district border to Awati district border",11),
            (5,"Pusad",1456894,551,"Shrirampur Vaijapur Risod Washim Pusad Mahagaon Fulsawangi Mandvi SH51",11),
            (6,"Amravati",2208067,196008,"Riddhapur Tiwsa Anjansinghi Dhamangaon Devgaon Yavatmal SH300",11),
            (7,"Daryapur",1520256,167010,"Walgaon Daryapur Akot SH47; Daryapur Amla Runmochan Asara MDR21 and SH301",12),
            (8,"Amravati",1884380,208451,"Chandur Railway Talegaon SH297; Riddhapur Tiwsa SH300",12),
            (9,"Solapur",1232286,35826,"Waradwadi Phata to Narewadi district border",12),
        ]
        for epc,piu,works,deposit,name,page in packages:
            attrs = scope | dict(entity_id=f"adb_52298_001_epc_{epc}",entity_name=f"MSRIP EPC{epc} / PIU{piu}",entity_type="project_package",asset_id=f"adb_52298_001_epc_{epc}",project_name=name)
            self.fact(sid,"project_civil_works_expenditure_inr_crore",works,"INR crore",f"PDF p{page}; Note6; EPC{epc} civil work executed",original_unit="INR thousand",start="2020-04-01",end="2025-05-19",basis="project_cumulative",asof="2025-05-19",statement="project_cash_basis_package_civil_works",notes=limits+"EPC package inside parent project; cannot be added to its reported aggregate. Road names retained as disclosed, not independently verified route geometry or legal asset ownership.",**attrs)
            self.fact(sid,"project_contractual_deposit_balance_inr_crore",deposit,"INR crore",f"PDF p11; Note5 Cash and Cash equivalent; EPC{epc} Deposit",original_unit="INR thousand",end="2025-05-19",basis="balance_sheet_snapshot",asof="2025-05-19",statement="project_cash_basis_contractual_deposits",notes=limits+"PIU retention/maintenance/PartV/security deposits payable under contract; not free cash or contractor debt. Nine printed package values sum928394 versus disclosed total928530 INRthousand; the unreconciled total is quarantined, with no synthetic balancing.",**attrs)
        target_scope = scope | dict(evidence="target",eligible=False)
        for metric,value,unit in [("target_project_road_length_km",450,"km"),("target_postconstruction_maintenance_years",5,"years")]:
            self.fact(sid,metric,value,unit,"PDF p7; Note1 Project nature and activities",end="2025-05-19",basis="project_plan_context",estimate="target",asof="2025-05-19",statement="project_objectives",notes="Project aims to upgrade about450km to two-lane standard and maintain improved assets for5years. These are project objectives; report closure does not measure450km delivery or completed maintenance. Report cutoff dates this disclosed context, not target achievement.",**target_scope)
        for metric,value,label in [("target_project_base_cost_inr_crore",15574867,"Base Cost"),("target_project_contingency_cost_inr_crore",1773246,"Contingencies"),("target_project_financing_charges_inr_crore",838313,"Financial Charges During Implementation"),("target_project_total_cost_inr_crore",18186426,"Total")]:
            self.fact(sid,metric,value,"INR crore",f"PDF p8; Note3(c) Project Cost; PAM financing arrangement; {label}",original_unit="INR thousand",end="2025-05-19",basis="project_financing_plan",estimate="target",asof="2025-05-19",statement="PAM_financing_plan",notes="Historical financing plan in final accounts; USD255.99million at INR71.0435/USD, October2019 prices. Original planned cost is not actual expenditure or current replacement value; report cutoff dates the disclosure, not a new plan approval.",**target_scope)
        self.notes[sid] = ("Final scanned MPWD project accounts visually checked on physical pages3-8,11-13,24-25. Exact PDF hash pinned; OCR digit errors are not ingested. "
                           "Cash basis; current1April2024-19May2025, priorFY2023-24, cumulative1April2020-19May2025. "
                           "Authorised20August2025; auditor22August2025; ADB document pagepublished5September2025. "
                           "Government of India borrower, ADB lender, MPWD executing agency. No state aggregate debt/guarantees inferred. "
                           "Printed dashes remain absent. Investment components differ from printed subtotal by one INRthousand. Nine printed package deposits sum928394 versus reported total928530 INRthousand, an unreconciled136-thousand discrepancy; no cause inferred. Reported assertions retained separately. "
                           "Project450km/5year-maintenance objectives and historical PAM cost plan are target context, excluded from measured arithmetic. "
                           "Automated ADB requests remain restricted; manual browser download archive used, no automatic refresh claim.")

    def netc_manual_snapshot(self) -> None:
        """Extract only monthly flows from the governed rendered official table."""
        sid = NETC_SOURCE_ID
        path = self.raw_root / NETC_RENDERED_SNAPSHOT
        if not path.exists():
            self.notes[sid] = "HTTP403/JS restricted primary page; governed rendered-table snapshot required; no invented API or numerical facts."
            return
        snapshot = json.loads(path.read_text())
        expected_headings = ["Month", "No. of Banks Live on NETC", "Tag Issuance (In Nos. BTD)", "Volume (In Mn.) MTD", "Amount (In Cr) MTD"]
        if snapshot.get("source_url") != DOCUMENTS[sid] or snapshot.get("headings") != expected_headings:
            raise ValueError("NETC snapshot source/headings changed; manual review required")
        captured = datetime.fromisoformat(snapshot["captured_at"])
        if captured.utcoffset() is None or not snapshot.get("capture_method") or not snapshot.get("published_exclusions"):
            raise ValueError("NETC rendered snapshot requires dated capture provenance and exclusions")
        if snapshot.get("publication_date") is not None:
            raise ValueError("NETC publication date is undisclosed in this capture")
        documents = []
        seen = set()
        for reporting_year in snapshot["reporting_years"]:
            for raw in reporting_year["rows"]:
                if len(raw) != 5:
                    raise ValueError("NETC rendered row must retain all five source cells")
                month = datetime.strptime(raw[0], "%B-%Y").date()
                end = month.replace(day=calendar.monthrange(month.year, month.month)[1]).isoformat()
                fiscal_start = month.year if month.month >= 4 else month.year - 1
                if reporting_year["selected_year"] != f"{fiscal_start}-{(fiscal_start+1)%100:02d}" or end in seen:
                    raise ValueError("NETC reporting-year/month mismatch or duplicate month")
                seen.add(end)
                volume, amount = number(raw[3]), number(raw[4])
                if not all(math.isfinite(value) and value >= 0 for value in (volume, amount)):
                    raise ValueError("NETC monthly flows require finite nonnegative source values")
                documents.append((reporting_year["selected_year"], raw, month.isoformat(), end, volume, amount))
        if len(documents) != 17 or max(seen) != snapshot["observation_cutoff"] or min(seen) != "2025-04-30":
            raise ValueError("NETC governed capture must cover April2025-August2026 with seventeen monthly rows")
        # The dated local JSON is the reproducibility artifact. Its hash never
        # purports to be a checksum of publisher HTML/PDF response bytes.
        self.documents[sid] = []
        self.rows[sid] = []
        self.pin(sid, path=path, url=snapshot["source_url"])
        self.documents[sid][0].update(artifact_kind=RENDERED_SNAPSHOT_KIND, checksum_scope="Local governed rendered-table JSON, not publisher HTML/PDF bytes", capture_method=snapshot["capture_method"], captured_at=snapshot["captured_at"], publisher_document_checksum_available=False)
        scope = "NETC payment transactions and scheme payment amounts are not NHAI toll receipts, vehicle counts, PCU traffic or corridor revenue."
        exclusions = snapshot["published_exclusions"]
        for selected_year, raw, start, end, volume, amount in documents:
            attrs = dict(entity_id="NPCI_NETC", entity_name="NPCI NETC payment network", entity_type="payment_network", agency="NPCI", road_class="NETC network (multiple road classes)", start=start, end=end, basis="calendar_month", asof=end, statement="NETC_payment_statistics", published="", evidence="issuer_disclosure", metric_category="issuer_disclosed", source_type="issuer", notes=f"{scope} Institutional issuer disclosure, not a government measurement. Published exclusions: {exclusions} Local source hash pins governed rendered-table JSON, not publisher PDF bytes. Raw volume '{raw[3]}' million transactions; raw amount '{raw[4]}' INR crore; reporting year {selected_year}. Bank-live and cumulative tag issuance are retained in the capture and not extracted as flows.")
            anchor = f"Rendered NETC FASTag Statistics; reporting year {selected_year}; row {raw[0]}"
            self.fact(sid, "netc_payment_transactions", volume, "transactions", anchor + "; Volume (In Mn.) MTD", original_unit="million transactions", **attrs)
            self.fact(sid, "netc_payment_amount_inr_crore", amount, "INR crore", anchor + "; Amount (In Cr) MTD", **attrs)
        self.notes[sid] = f"Governed manual browser capture of seventeen official monthly rows, April2025-August2026, captured{snapshot['captured_at']}. Publication date undisclosed. {scope} Published exclusions: {exclusions} Document SHA256 pins local rendered JSON, not publisher response bytes; future months require a new reviewed capture."

    def finish(self, source_ids: tuple[str, ...] = SOURCE_IDS) -> None:
        manual=self.raw_root/"manual"; (manual/"evidence").mkdir(parents=True,exist_ok=True)
        for sid in source_ids:
            rows=self.rows[sid]
            path=manual/f"{sid}.csv"
            with path.open("w",newline="") as stream:
                writer=csv.DictWriter(stream,fieldnames=FACT_COLUMNS,lineterminator="\n");writer.writeheader();writer.writerows(rows)
            retrieval_times=[doc.get("captured_at") or datetime.fromtimestamp((self.raw_root/doc["relative_path"]).stat().st_mtime,timezone.utc).isoformat() for doc in self.documents[sid]]
            evidence={"source_id":sid,"research_cutoff":RESEARCH_CUTOFF,"retrieved_at":max(retrieval_times) if retrieval_times else None,"last_checked_at":datetime.now(timezone.utc).isoformat(),"documents":self.documents[sid],"csv_sha256":sha256_for_file(path),"extraction_status":"validated" if rows else "evidence_gap","row_count":len(rows),"notes":self.notes.get(sid,""),"gap_reason":"" if rows else self.notes.get(sid,"No verified extract available; source remains a visible gap."),"transformation":"pipelines.connectors.primary_disclosures.SnapshotBuilder; raw units preserved and INR crore normalized; physical PDF pages are one-based"}
            if any(doc.get("artifact_kind") == RENDERED_SNAPSHOT_KIND for doc in self.documents[sid]):
                evidence["transformation"] = "SnapshotBuilder.netc_manual_snapshot; original million transactions multiplied by1,000,000; INR crore unchanged; rendered table row/heading anchors; document hash pins governed local JSON rather than publisher response bytes."
            if rows:
                validate_facts(pd.read_csv(path,keep_default_na=False),sid,evidence)
            write_json(evidence,manual/"evidence"/f"{sid}.json")


def build_snapshots(raw_root: Path) -> SnapshotBuilder:
    builder=SnapshotBuilder(raw_root)
    for sid in SOURCE_IDS:
        if sid != NETC_SOURCE_ID and document_path(raw_root,sid).exists():
            builder.pin(sid)
    builder.notes.update({"npci_netc_monthly_statistics":"HTTP403/JS restricted primary page; governed manual snapshot required; no invented API or search-result numerical facts.","upeida_expressway_projects":"Verified TLS retrieval failed due to missing certificate issuer; Ganga progress PDF link returns500. No unverified project progress/financial facts.","msrdc_financial_disclosures":"Public index/subsidiary FY23-24 filings identified; primary download timed out. Parent standalone latest visible FY18-19, not FY23-24. No statement facts without retrieved PDF.","adb_state_road_projects":"Official project52298-001 primary page HTTP403; financial statement April2024-May2025 disclosed in index; no unretrieved numerical facts.","rbi_state_road_finances":"Goa revenue Roads and Bridges rows contain dashes, left absent rather than invented zero. Road-corporation debt/guarantees and corridor attribution not inferred from state-government aggregates."})
    for function in (builder.budget,builder.nhit,builder.parliament_and_audit,builder.monetisation,builder.upeida,builder.nhidcl,builder.rbi,builder.brs,builder.netc_manual_snapshot,builder.adb_project_accounts):
        function()
    builder.finish()
    return builder


if __name__ == "__main__":
    parser=argparse.ArgumentParser(description="Rebuild verified primary financial snapshots from archived PDFs")
    parser.add_argument("--raw-root",type=Path,default=Path("data/raw"))
    args=parser.parse_args()
    snapshot=build_snapshots(args.raw_root)
    print(json.dumps({sid:len(rows) for sid,rows in snapshot.rows.items()},indent=2))
