# Finance enrichment: baseline and delivery evidence

## Baseline captured before implementation

Baseline: main commit `f873a84bf8d327c3b739eb4c055546a48b237685`.
Human inventory v1 contains 34 sources (21 automatic, 13 governed manual); machine inventory contains 29; catalog contains 35 datasets, generated 26 March 2026. All existing source IDs and URLs are retained in `research/source_inventory.yaml`; the generated inventory will be reconciled against it. Catalog categories: 31 official, 2 proxy, 2 model. Its 221,176 rows include 219,996 synthetic scenario rows and 190 correlations with only two or three overlapping observations.

Published baseline: https://ganesh47.github.io/bharat-highway-asset-intelligence/ and `methodology.html`. A browser screenshot and accessibility snapshot were captured during planning. Default mode: All Signals, Normal density, All states. Visible cards report 31 official, four proxy, zero model, and 221,176 rows; all chart confidence badges are Low. The State Portfolio mixes stock/flow/portfolio lengths; Budget vs Expenditure does not distinguish the January 2025 cutoff. Existing construction, safety, CRIF, appendices, ontology, citations, chart labels/units/date/legend/accessibility checks must remain or improve.

Research workflow is disabled for inactivity; last research run 32455267508 failed OCR imports. Artifact ZIP restoration paths and CD consumption are incorrect. A successful refresh must deploy its own validated catalog, not old checkout data.

## Approved scope and defaults

Infrastructure and finance analysts; National and State Highways and state expressways. Broad comparable official state coverage, with project/finance depth where primary disclosures support it. Research cutoff 2 October 2026. Use government, issuer and multilateral primary disclosures; keep unavailable sources as visible gaps. Preserve actual/BE/RE/YTD, entity/statement basis, original units, observation dates, and document/table/page lineage. Roads and Bridges expenditure is broader than State Highways; report-year dates do not override older per-state observations.

## Delivery evidence

Three implementation agents covered source research/extraction, pipeline integrity, and frontend/browser verification; the coordinating agent repaired delivery workflows, added state debt/guarantees and financial reconciliation checks, and integrated the outputs.

The reconciled inventory contains **50 sources** and the catalog **51 datasets** (the extra dataset is the governed correlation artifact). Sixteen added definitions contribute **4,076 cited disclosure facts**. Current local publication has 34 sources with readable verified disclosures and 32 with calculation-eligible observations. Targets, valuations, BE/RE, historical audit samples, undated project context and measured actuals retain their separate evidence labels.

Every registered source is accounted for in `data/manifests/refresh_report.json` and [coverage_matrix.md](../coverage_matrix.md): 18 updated, 18 checked unchanged, 10 require manual evidence, two are restricted, one is metadata-only, and one retains its supported observations after a failed live check. NPCI and ADB remain restricted gaps; MSRDC remains a manual evidence gap. RBI's verified 1,302-row snapshot remains published with its original dates when the live endpoint returns a challenge response instead of a PDF.

New debt and guarantees use RBI Statements 19 and 28 with state/year cell coordinates to prevent missing cells shifting later years. State all-sector balances remain separate from Roads and Bridges spending and road corporations. Missing guarantees remain absent; Jammu and Kashmir's apportioned-liability footnote is retained. Budget gross plus signed recoveries reconciles to net for each actual/BE/RE column; NHIT distribution components reconcile in INR per unit.

The Tamil Nadu OGD project CSV lost decimal points in 33 progress cells, including `67` instead of `0.67`. Its correction verifies all 55 project identities, agencies, dates, lengths and costs against the checksum-pinned parliamentary Annexure II of 24 July 2024. Original CSV values, corrected values and both document hashes remain in lineage. Two undisclosed progress cells remain missing.

Synthetic data and unsupported manual observations remain outside default measured totals. The former 190 correlations with two or three matches are replaced by two approved comparable pairs (107 matched safety observations and 32 matched project-delay observations); neither carries a causal interpretation.

Local validation includes source failure/unchanged preservation, inventory reconciliation, evidence/checksum contracts, OCR failure retention, monetary units, net/gross and distribution reconciliation, entity/estimate boundaries, source exclusions and scoped duplicates. Playwright passes all 13 existing government chart checks plus issuer/model filters, state spending actual/BE/RE, state guarantees, NHIT contractual maturities, targets/valuation qualifications, citations, CSV lineage and mobile overflow checks.

The research workflow is re-enabled on its original daily schedule (06:00 UTC / 11:30 IST). It restores prior validated data without overwriting current governed extracts, handles OCR/no-OCR failure gates, synchronizes inventory readiness without re-probing, and archives raw evidence. Deployment restores the triggering research artifact and verifies the published catalog checksum before browser checks. The first repair CI run is [36915191637](https://github.com/ganesh47/bharat-highway-asset-intelligence/actions/runs/36915191637), successful. Final Actions and deployment evidence follows below.
