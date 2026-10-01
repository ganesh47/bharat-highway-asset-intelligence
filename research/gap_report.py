from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from .loader import load_inventory
from pipelines.common import read_json


MANDATORY_THEMES = [
    "projects",
    "finance",
    "toll_fastag",
    "safety",
    "macro",
    "procurement_awards",
    "contractors",
    "arbitration_claims",
    "quality_maintenance_signals",
]

OFFICIAL_REMEDIATION = {
    "projects": "Use MoRTH/NHAI annual publications, PMGSY and State PWD handover statements.",
    "finance": "Prioritize Union Budget demands, NHAI annual statements, and PIB release tables.",
    "toll_fastag": "Use MoRTH / Ministry of Finance circulars and official FASTag portal disclosures. Only use telemetry summaries as proxy when cited explicitly.",
    "safety": "Pull state/year tables from NCRB official annual publications and Road Accident query tables.",
    "macro": "Use RBI/MOSPI open download endpoints with official methodology notes and circular references.",
    "procurement_awards": "Use Parliament replies, CVC disclosures, and CAG review points for restricted procurement pipelines.",
    "contractors": "Cross-check NHAI award/award-cancel notices and audited procurement records where available.",
    "arbitration_claims": "Track official Parliamentary Q&A, ministry replies, audit paras, and tribunal orders.",
    "quality_maintenance_signals": "Use official QA/QC acceptance and maintenance completion documents as primary substitutes to proxy-only map data.",
}


def detect_gaps(inventory: List[Dict[str, Any]], catalog: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    by_theme: Dict[str, list] = {}
    for item in inventory:
        by_theme.setdefault(item.get("theme", "unknown"), []).append(item)
    catalog = catalog or {}
    gaps = []

    for theme in MANDATORY_THEMES:
        sources = by_theme.get(theme, [])
        if not sources:
            gaps.append(
                {
                    "theme": theme,
                    "missing_reason": "No source currently cataloged in official inventory",
                    "suggested_official_avenues": OFFICIAL_REMEDIATION.get(theme, "Check Parliament Q&A archives and CAG audit reports."),
                    "recommended_status": "high",
                }
            )
            continue

        blocked = [source for source in sources if source.get("auth") in {"captcha", "restricted"} or source.get("allow_auto_fetch") is False]
        pending = [source for source in sources if source["source_id"] in catalog and catalog[source["source_id"]].get("analytical_ready") is not True]
        affected = {source["source_id"] for source in blocked + pending}
        if affected:
            gaps.append(
                {
                    "theme": theme,
                    "missing_reason": f"{len(affected)} of {len(sources)} sources require manual evidence, validated extraction, or restricted access; discovery is not analytical coverage",
                    "source_ids": sorted(affected),
                    "suggested_official_avenues": OFFICIAL_REMEDIATION.get(
                        theme,
                        "Request official release channel access or place curated exports into data/raw/manual with provenance notes.",
                    ),
                    "recommended_status": "evidence_required",
                }
            )

    return gaps


def write_gaps_markdown(gaps: List[Dict[str, Any]], out_path: str = "research/gaps.md") -> Path:
    lines = [
        "# Source Gap Analysis\n",
        f"Generated: {datetime.now(timezone.utc).isoformat()} UTC\n",
        "Tracks missing dimensions before ETL and recommends official avenues to fill them.\n",
        "## Missing dimensions\n",
    ]

    if not gaps:
        lines.append("- No tracked theme gaps identified in the current inventory.\n")
    else:
        for gap in gaps:
            lines.extend(
                [
                    f"- Theme: **{gap['theme']}**",
                    f"  - Missing reason: {gap['missing_reason']}",
                    f"  - Suggested action: {gap['suggested_official_avenues']}",
                    f"  - Priority: {gap['recommended_status']}\n",
                ]
            )
            if gap.get("source_ids"):
                lines.append(f"  - Sources: {', '.join(gap['source_ids'])}\n")

    lines.append("## Mandatory operating rules\n")
    lines.append("- Keep auto-fetch off for restricted/captcha sources.")
    lines.append("- Keep `research/source_inventory.yaml` as human approval gate.")
    lines.append("- Log connector capability and known limitations in the source notes.")
    lines.append("- State-wise National Highway statistics do not establish State Highway coverage.")
    lines.append("- Roads and Bridges expenditure covers a wider functional category than State Highways.")
    lines.append("- Keep issuer accounts, government budgets, targets, valuations and historical audit samples distinct.")

    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines), encoding="utf-8")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate official data gap report")
    parser.add_argument("--inventory", default="research/source_inventory.yaml")
    parser.add_argument("--out", default="research/gaps.md")
    args = parser.parse_args()

    inv = load_inventory(args.inventory)
    catalog = {entry["source_id"]: entry for entry in read_json(Path("data/manifests/catalog.json")).get("datasets", [])}
    gaps = detect_gaps(inv.sources, catalog)
    path = write_gaps_markdown(gaps, args.out)
    print(f"Wrote gap report: {path} ({len(gaps)} items)")


if __name__ == "__main__":
    main()
