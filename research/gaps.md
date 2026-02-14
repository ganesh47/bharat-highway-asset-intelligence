# Source Gap Analysis

Generated: 2026-02-14T06:47:20.701943+00:00 UTC

Tracks missing dimensions before ETL and recommends official avenues to fill them.

## Missing dimensions

- Theme: **projects**
  - Missing reason: Source exists but automated retrieval is restricted
  - Suggested action: Use MoRTH/NHAI annual publications, PMGSY and State PWD handover statements.
  - Priority: manual_first

- Theme: **finance**
  - Missing reason: Source exists but automated retrieval is restricted
  - Suggested action: Prioritize Union Budget demands, NHAI annual statements, and PIB release tables.
  - Priority: manual_first

- Theme: **toll_fastag**
  - Missing reason: Source exists but automated retrieval is restricted
  - Suggested action: Use MoRTH / Ministry of Finance circulars and official FASTag portal disclosures. Only use telemetry summaries as proxy when cited explicitly.
  - Priority: manual_first

- Theme: **safety**
  - Missing reason: Source exists but automated retrieval is restricted
  - Suggested action: Pull state/year tables from NCRB official annual publications and Road Accident query tables.
  - Priority: manual_first

- Theme: **macro**
  - Missing reason: Source exists but automated retrieval is restricted
  - Suggested action: Use RBI/MOSPI open download endpoints with official methodology notes and circular references.
  - Priority: manual_first

- Theme: **procurement_awards**
  - Missing reason: Source exists but automated retrieval is restricted
  - Suggested action: Use Parliament replies, CVC disclosures, and CAG review points for restricted procurement pipelines.
  - Priority: manual_first

- Theme: **contractors**
  - Missing reason: Source exists but automated retrieval is restricted
  - Suggested action: Cross-check NHAI award/award-cancel notices and audited procurement records where available.
  - Priority: manual_first

- Theme: **arbitration_claims**
  - Missing reason: Source exists but automated retrieval is restricted
  - Suggested action: Track official Parliamentary Q&A, ministry replies, audit paras, and tribunal orders.
  - Priority: manual_first

- Theme: **quality_maintenance_signals**
  - Missing reason: Source exists but automated retrieval is restricted
  - Suggested action: Use official QA/QC acceptance and maintenance completion documents as primary substitutes to proxy-only map data.
  - Priority: manual_first

## Mandatory operating rules

- Keep auto-fetch off for restricted/captcha sources.
- Keep `research/source_inventory.yaml` as human approval gate.
- Log connector capability and known limitations in the source notes.