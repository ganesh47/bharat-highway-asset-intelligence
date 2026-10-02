"""Document source accountability and analytical coverage without summing unlike scopes."""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from research.loader import load_inventory

STATES = [
    "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh", "Goa", "Gujarat",
    "Haryana", "Himachal Pradesh", "Jharkhand", "Karnataka", "Kerala", "Madhya Pradesh",
    "Maharashtra", "Manipur", "Meghalaya", "Mizoram", "Nagaland", "Odisha", "Punjab",
    "Rajasthan", "Sikkim", "Tamil Nadu", "Telangana", "Tripura", "Uttar Pradesh", "Uttarakhand",
    "West Bengal", "Andaman and Nicobar Islands", "Chandigarh",
    "Dadra and Nagar Haveli and Daman and Diu", "Delhi", "Jammu and Kashmir", "Ladakh",
    "Lakshadweep", "Puducherry",
]
ALIASES = {"NCT of Delhi": "Delhi", "National Capital Territory of Delhi": "Delhi",
           "NCT Delhi": "Delhi", "Andaman & Nicobar Island": "Andaman and Nicobar Islands",
           "Andaman & Nicobar Islands": "Andaman and Nicobar Islands", "Orissa": "Odisha",
           "Uttaranchal": "Uttarakhand", "Jammu & Kashmir": "Jammu and Kashmir",
           "Dadra & Nagar Haveli and Daman & Diu": "Dadra and Nagar Haveli and Daman and Diu"}


def cell(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "Unknown"
    return str(value).replace("|", "\\|").replace("\n", " ").replace("\r", " ")


def dates(df: pd.DataFrame, fallback: str | None = None) -> str:
    columns = ["data_as_of"] if {"data_as_of", "source_document_sha256"} <= set(df.columns) else ["data_as_of", "source_as_of_date", "period_end", "year"]
    for column in columns:
        if column in df:
            values = sorted(set(df[column].dropna().astype(str)) - {"", "nan", "None"})
            if values:
                return values[0] if len(values) == 1 else f"{values[0]} to {values[-1]}"
    if {"data_as_of", "source_document_sha256"} <= set(df.columns):
        return "Unknown"
    return fallback or "Unknown"


def coverage_buckets(frames: dict[str, pd.DataFrame]) -> list[set[str]]:
    """Discover measured scopes from metrics rather than freezing old source IDs."""
    buckets = [{"morth_annual_report_pdf"}, set(), set(),
               {"nhidcl_monthly_project_progress", "upeida_expressway_projects",
                "msrdc_financial_disclosures", "adb_state_road_projects", "mospi_paimana_monthly_projects"}]
    for sid, frame in frames.items():
        metrics = set(frame["metric"].dropna().astype(str)) if "metric" in frame else set()
        if "nh_network_length_km" in metrics:
            buckets[0].add(sid)
        if metrics & {"sh_network_length_km", "sh_surfaced_length_km"}:
            buckets[1].add(sid)
        if any(metric.startswith("roads_bridges_") for metric in metrics):
            buckets[2].add(sid)
        if "entity_type" in frame and frame["entity_type"].eq("project").any() and "state" in frame:
            buckets[3].add(sid)
    return buckets


def build(inventory_path: Path, catalog_path: Path, out: Path, root: Path = ROOT) -> dict:
    inventory = load_inventory(inventory_path)
    sources = {item["source_id"]: item for item in inventory.sources}
    catalog = json.loads(catalog_path.read_text())
    entries = {item["source_id"]: item for item in catalog.get("datasets", [])}
    missing = sources.keys() - entries.keys()
    if missing:
        raise ValueError(f"Sources missing from catalog: {sorted(missing)}")
    report_path = catalog_path.parent / "refresh_report.json"
    refresh = json.loads(report_path.read_text()) if report_path.exists() else {}
    outcomes = {item["source_id"]: item for item in refresh.get("sources", [])}
    if sources.keys() - outcomes.keys():
        raise ValueError("Every registered source requires a recorded refresh outcome")
    frames = {}
    for sid, entry in entries.items():
        if entry.get("analytical_ready") is not True or entry.get("metric_category") == "model_output":
            continue
        path = Path(entry.get("output_table_path", ""))
        if not path.is_absolute():
            path = root / path
        if not path.is_file():
            continue
        frame = pd.read_parquet(path)
        if "analytical_eligible" in frame:
            frame = frame[frame["analytical_eligible"].eq(True)]
        frames[sid] = frame
    summary = {"registered_sources": len(sources), "published_datasets": len(entries),
               "disclosure_ready_sources": sum(entry.get("disclosure_ready") is True for entry in entries.values()),
               "ready_sources": len(frames), "ready_rows": sum(len(df) for df in frames.values()),
               "refresh_outcomes": dict(Counter(row["outcome"] for row in outcomes.values()))}
    lines = ["# Highway intelligence coverage", "", "Research cutoff: **2 October 2026**. Observation dates below remain those of the disclosures.", "",
             f"{summary['registered_sources']} registered sources; {summary['published_datasets']} published datasets; "
             f"{summary['disclosure_ready_sources']} sources with verified readable disclosures, including clearly tagged targets/valuations; "
             f"{summary['ready_sources']} sources with observations eligible for measured calculations. Eligible rows are heterogeneous observations, not a network-size or project-count measure.", "",
             "A retained dataset can remain useful after a failed check. Read its observation cutoff and failure together. Unknown and unavailable values are gaps; they are never substituted with zero.", "",
             "## Source-by-source refresh and extraction matrix", "",
             "| Source ID | Publisher / title | Outcome | Analytical use | Rows | Observation cutoff | Publication | Extraction / gap |",
             "|---|---|---|---|---:|---|---|---|"]
    for sid, source in sources.items():
        entry, outcome = entries[sid], outcomes[sid]
        title = cell(source.get("dataset_title", sid))
        url = source.get("resource_page_url") or source.get("url")
        title = f"[{title}]({url})" if url else title
        reason = entry.get("refresh_error") or entry.get("skip_reason") or entry.get("quarantine_reason")
        extraction = entry.get("extraction_status", "Unknown")
        if reason:
            extraction += ": " + str(reason)
        lines.append("| " + " | ".join(map(cell, [sid, title, outcome["outcome"],
                    "Measured observations" if sid in frames else "Disclosures only" if entry.get("disclosure_ready") else "Excluded / gap", entry.get("manifest", {}).get("row_count", 0),
                    dates(frames[sid], entry.get("source_as_of_date")) if sid in frames else entry.get("source_as_of_date"),
                    entry.get("publication_date"), extraction])) + " |")
    extra = entries.keys() - sources.keys()
    if extra:
        lines.extend(["", "## Generated datasets", "", "| Dataset | Rows | Use |", "|---|---:|---|"])
        for sid in sorted(extra):
            entry = entries[sid]
            lines.append(f"| {cell(sid)} | {entry.get('manifest', {}).get('row_count', 0)} | {cell(entry.get('citations', {}).get('note', 'Derived artifact'))} |")
    lines.extend(["", "## State and union territory coverage", "",
                  "Each cell lists disclosed metrics and their own periods. Project portfolios are not highway network stocks. Roads and Bridges expenditure has a broader scope than State Highways. A zero published in a primary table is a reported value; an empty cell below means no eligible observations.", "",
                  "| State / UT | NH network stock | SH network stock | Roads and Bridges spending | Detailed corridor / delivery evidence |",
                  "|---|---|---|---|---|"])
    buckets = coverage_buckets(frames)
    for state in STATES:
        values = []
        for bucket_index, bucket in enumerate(buckets):
            evidence = []
            for sid in sorted(bucket & frames.keys()):
                df = frames[sid]
                state_col = "state" if "state" in df else "state_name" if "state_name" in df else None
                if not state_col:
                    continue
                part = df[df[state_col].map(lambda value: ALIASES.get(str(value), str(value))).eq(state)]
                historical_regions = []
                if state == "Dadra and Nagar Haveli and Daman and Diu" and sid == "morth_state_highway_network":
                    part = df[df[state_col].isin(["Dadra & Nagar Haveli", "Daman & Diu"])]
                    historical_regions = ["Separate historical entities; no combined stock inferred"]
                if bucket_index == 2 and "metric" in part:
                    part = part[part["metric"].str.startswith("roads_bridges_")]
                if bucket_index == 0 and "metric" in part:
                    part = part[part["metric"].eq("nh_network_length_km")]
                if bucket_index == 1 and "metric" in part:
                    part = part[part["metric"].isin(["sh_network_length_km", "sh_surfaced_length_km"])]
                if sid == "morth_annual_report_pdf" and "metric_name" in part:
                    part = part[part["metric_name"].eq("appendix2_statewise_nh_length_km")]
                if part.empty:
                    continue
                metric_col = "metric" if "metric" in part else "metric_name" if "metric_name" in part else None
                metrics = sorted(part[metric_col].dropna().astype(str).unique()) if metric_col else ["network length"]
                evidence.append(f"{sid}: {', '.join(metrics)} ({dates(part, entries[sid].get('source_as_of_date'))})" + (". " + "; ".join(historical_regions) if historical_regions else ""))
            values.append("; ".join(evidence) or "Gap")
        lines.append("| " + " | ".join(map(cell, [state, *values])) + " |")
    lines.extend(["", "## Boundaries and remaining gaps", "",
                  "- Budgets retain actual, revised estimate and budget estimate separately. Utilisation requires matching entity, fiscal period, expenditure scope and cutoff.",
                  "- NHIT accounts and valuations describe the trust and disclosed SPVs. They do not measure NHAI standalone debt or all-India toll receipts.",
                  "- NPCI NETC payment volumes/amounts are payment-system statistics; published exclusions remain part of their scope. Corridor PCU traffic, transaction counts and toll revenue are separate metrics.",
                  "- State expressway costs and audited SPV financial statements remain gaps where primary documents could not be retrieved or verified. State Roads and Bridges spending cannot fill those gaps.",
                  "- Unverified manual records remain in quarantine for review; synthetic models remain demonstrations. Neither enters measured totals or approved correlations.",
                  "- Correlations require an explicitly approved pair and at least ten matched comparable observations. Their sample counts accompany the results; no causal claim is implied.", ""])
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines))
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, default=ROOT / "research/source_inventory.yaml")
    parser.add_argument("--catalog", type=Path, default=ROOT / "data/manifests/catalog.json")
    parser.add_argument("--out", type=Path, default=ROOT / "docs/coverage_matrix.md")
    args = parser.parse_args()
    print(json.dumps(build(args.inventory, args.catalog, args.out), indent=2))
