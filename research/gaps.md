# Source Gap Analysis

Generated: 2026-10-01T20:22:54.164172+00:00 UTC

Tracks missing dimensions before ETL and recommends official avenues to fill them.

## Missing dimensions

- Theme: **projects**
  - Missing reason: 2 of 15 sources require manual evidence, validated extraction, or restricted access; discovery is not analytical coverage
  - Suggested action: Use MoRTH/NHAI annual publications, PMGSY and State PWD handover statements.
  - Priority: evidence_required

  - Sources: nhai_constructed_length_series_official, nhai_press_release_index

- Theme: **finance**
  - Missing reason: 3 of 8 sources require manual evidence, validated extraction, or restricted access; discovery is not analytical coverage
  - Suggested action: Prioritize Union Budget demands, NHAI annual statements, and PIB release tables.
  - Priority: evidence_required

  - Sources: morth_annual_report_pdf, nhai_annual_report_documents, nhai_audited_results_pdf

- Theme: **toll_fastag**
  - Missing reason: 1 of 1 sources require manual evidence, validated extraction, or restricted access; discovery is not analytical coverage
  - Suggested action: Use MoRTH / Ministry of Finance circulars and official FASTag portal disclosures. Only use telemetry summaries as proxy when cited explicitly.
  - Priority: evidence_required

  - Sources: ncrb_toll_fastag_claims

- Theme: **safety**
  - Missing reason: 4 of 8 sources require manual evidence, validated extraction, or restricted access; discovery is not analytical coverage
  - Suggested action: Pull state/year tables from NCRB official annual publications and Road Accident query tables.
  - Priority: evidence_required

  - Sources: highway_project_risk_and_access_panel, ncrb_road_accidents_state_year, parliament_qa_highway_queries, parliament_qa_nh_blackspots_state

- Theme: **macro**
  - Missing reason: 1 of 2 sources require manual evidence, validated extraction, or restricted access; discovery is not analytical coverage
  - Suggested action: Use RBI/MOSPI open download endpoints with official methodology notes and circular references.
  - Priority: evidence_required

  - Sources: rbi_mospi_macro_indicators

- Theme: **procurement_awards**
  - Missing reason: 1 of 1 sources require manual evidence, validated extraction, or restricted access; discovery is not analytical coverage
  - Suggested action: Use Parliament replies, CVC disclosures, and CAG review points for restricted procurement pipelines.
  - Priority: evidence_required

  - Sources: morh_procurement_awards

- Theme: **contractors**
  - Missing reason: 1 of 1 sources require manual evidence, validated extraction, or restricted access; discovery is not analytical coverage
  - Suggested action: Cross-check NHAI award/award-cancel notices and audited procurement records where available.
  - Priority: evidence_required

  - Sources: morh_contractor_disclosures

- Theme: **arbitration_claims**
  - Missing reason: 1 of 1 sources require manual evidence, validated extraction, or restricted access; discovery is not analytical coverage
  - Suggested action: Track official Parliamentary Q&A, ministry replies, audit paras, and tribunal orders.
  - Priority: evidence_required

  - Sources: morh_arbitration_claims

- Theme: **quality_maintenance_signals**
  - Missing reason: 2 of 2 sources require manual evidence, validated extraction, or restricted access; discovery is not analytical coverage
  - Suggested action: Use official QA/QC acceptance and maintenance completion documents as primary substitutes to proxy-only map data.
  - Priority: evidence_required

  - Sources: quality_maintenance_indicators, viirs_nightlights_proxy

## Mandatory operating rules

- Keep auto-fetch off for restricted/captcha sources.
- Keep `research/source_inventory.yaml` as human approval gate.
- Log connector capability and known limitations in the source notes.
- State-wise National Highway statistics do not establish State Highway coverage.
- Roads and Bridges expenditure covers a wider functional category than State Highways.
- Keep issuer accounts, government budgets, targets, valuations and historical audit samples distinct.