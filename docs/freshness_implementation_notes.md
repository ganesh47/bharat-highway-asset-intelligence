# Freshness implementation — 2 October 2026

The baseline at `1a0157d` contained 50 registered sources and 51 datasets: 30 known observation cutoffs, 20 unknown cutoffs, and nine ending in 2023 or earlier. The source inventory, catalog, schemas and previous release checks were inspected before implementation. The deployed baseline had 13 government charts and seven analyst themes. Its fixed safety, GSDP and state-construction column selections prevented newer observations from appearing. Baseline screenshot and filter capture: `buildcheck/freshness-before.png` and `.json`.

Three agents handled national disclosures, state and statistical evidence, and pipeline/history validation. The integration agent handled PAIMANA, the frontend and release verification. Source IDs, units, citations, confidence gates and analytical exclusions are preserved. New publications have separate source identities; they do not silently replace older vintages.

The current calendar quarter is October–December 2026, fiscal Q3 FY2026–27. The most recent closed quarter ended 30 September. A quarter ending does not establish publication of its results. PAIMANA announces its September report for 26 October. The rendered NPCI table and NHIDCL index still show August observations. NHIT's rendered disclosures list June-quarter results; September-quarter statements were not listed when checked. Newer audited NHAI accounts and debt observations remain explicit publication gaps.

Five undated legacy project sources (358 rows) are retained for verification and excluded from dated rankings and trends. The old construction series and governed NHAI annual series disagree in six of eight overlapping fiscal years. They remain separate; there is no spliced aggregate. Historical CAG audits, closed ADB projects and NIP forecasts keep their historical context.

GSDP charts use the period with the greatest compatible entity coverage among the three latest observed annual periods, preferring the latest in a tie. The evidence explorer offers all history, latest per entity and common-period selections. Monetary flows require three unique compatible months to form a quarter; incomplete quarters, cumulative project spending and stocks cannot be summed as quarterly flows.

Publication discovery produces candidate links rather than numerical facts. Document and extraction checksums gate publication. Immutable document and fact archives retain previous periods and revisions. Unknown publication dates stay unknown. A newer source-wide date never changes an older observation's date.

The state-accounts catalogue audit covers all 36 jurisdictions. Indexed statements awaiting extraction are labelled extraction pending, separately from restricted retrieval and missing releases. Account heads 3054 and 5054 cover Roads and Bridges across road classes; their expenditure is not presented as SH-only spending. MoRTH's newer national network total differs from its state sum by 2 km; the published total remains visible but excluded from reconciliation-based calculations.

Release evidence and final coverage counts are recorded after CI and deployed-site validation.
