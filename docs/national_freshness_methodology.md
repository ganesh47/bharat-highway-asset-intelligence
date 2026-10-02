# National series recency and assurance

Research cutoff: **2 October 2026**. These four source families add **2,062 cited facts**, of which **2,011 are eligible for calculations**. Their periods are the periods actually reported, rather than the year in a report filename or its retrieval date. Existing historical sources and their earlier publication vintages remain available.

| Source family | Facts / eligible | Latest observation | Publication | Assurance and limits |
| --- | ---: | --- | --- | --- |
| [Road Accidents in India 2024](https://morth.gov.in/backend/documents/uploaded/1781177676_V1gUW8tJWT.pdf) | 1,776 / 1,776 | Calendar 2024, ending 31 December 2024 | Day unverified; official portal featured date 11 June 2026 retained as discovery metadata | Final statistical counts for all roads, NH including Expressways, and SH; 2021–2024 comparatives belong to this report vintage. |
| [NHAI FY2025–26 performance, PIB release 2247870](https://www.pib.gov.in/PressReleasePage.aspx?PRID=2247870&lang=1&reg=3) | 5 / 4 | Complete FY2025–26, ending 31 March 2026 | 1 April 2026 | Administrative reported actuals; construction target is a separate, ineligible disclosure. These are not audited accounts. |
| [MoRTH Annual Report 2025–26](https://morth.gov.in/backend/documents/uploaded/RTH%20Annual%20Report%20English.pdf) | 207 / 157 | Network stock and fiscal YTD through 31 December 2025; historical CRIF releases retain their own year ends | 30 March 2026, exact official reports API record | Selected scanned tables reviewed visually. Unknown allocation/BE vintages and unreconciled published network aggregates remain visible but excluded from arithmetic. |
| [NHAI results at 31 March 2025](https://nhai.gov.in/nhai/sites/default/files/mix_file/Q_F_f_tQended_on_31-March-2025.pdf) | 74 / 74 | FY2024–25 annual cash flows and 31 March 2025 balances; earlier comparatives retain their own dates | Day unverified | Current figures are unaudited with limited review; FY2023–24 column is labelled audited, December 2024 debt comparator unaudited. No later quarter is inferred. |

## Extraction and evidence

`pipelines/national_freshness.py` extends the governed primary builder. It verifies the exact downloaded document SHA256 before extraction, checks a committed reviewed-table JSON against its own checksum and publisher document hash, and emits the common fact schema with original units, citations and physical PDF pages. The report PDFs remain in ignored raw caches and content-addressed archives; governed CSVs, reviewed cell transcriptions, evidence sidecars and publication history are committed. New unreviewed OCR output cannot replace a validated snapshot.

Rebuild these four snapshots offline after restoring their pinned raw documents:

```sh
.venv/bin/python -m pipelines.national_freshness --raw-root data/raw --cutoff 2026-10-02
.venv/bin/python -m unittest tests.test_national_freshness -v
```

Absent raw documents produce explicit extraction gaps and preserve earlier validated extracts. Changed source bytes or edited reviewed cells fail closed and require another documented review. Official MoRTH discovery records are retained in `national_freshness_official_listing_records.json`, with the discovery API response hashes. A portal featured date is not promoted to publication or observation date.

## Comparability safeguards

The safety extract covers 12 annexures: 2–5 for all roads, 9–12 for National Highways, and 14–17 for State Highways. It includes 36 state/UT rows and a published national total for four calendar years. All 48 state-column sums reconcile exactly. Ranks, shares and population/vehicle/road density columns are excluded. A fatal accident counts an accident; a fatality counts a person. NH includes Expressways and must not be combined with the all-roads totals. The newer report's SH accident comparator for 2023 is 105,662, while the prior report vintage contains a different figure; the two vintages are preserved rather than overwritten.

NHAI's administrative FY2025–26 funding reconciles: ₹244,362 crore capital expenditure equals ₹238,384 crore Government Budgetary Support plus ₹5,978 crore own resources. NHAI constructed 5,313 km against a disclosed 4,640 km target. Own resources are not assumed to equal gross toll receipts. MoRTH-wide construction/award rows are separate from NHAI-only performance.

The annual report's Appendix 2 has 36 state labels whose network lengths total 146,570 km, while the printed national total is 146,572 km. The 2 km difference is retained explicitly, and the national aggregate is excluded from arithmetic. State highway-number counts overlap across states; the published national count of 670 is not derived by summing them. Separate Dadra and Nagar Haveli and Daman and Diu rows remain as published; Lakshadweep is absent. Appendix 3 includes 26 years of CRIF allocations/releases. Releases are funding flows, not construction achievement or expenditure. The current release is explicitly through 31 December 2025. Allocation approval vintages are unknown and remain ineligible.

Appendix 5 records National Permit fee disbursement from April to December 2025 in **absolute INR**, not crore: 33 state amounts reconcile to INR18,723,045,000, normalized to ₹1,872.3045 crore. This is a transport permit-fee distribution, not toll revenue or SH-only expenditure. Appendix 6 preserves all 22 major-head/gross/recovery/net rows and their BE/YTD columns separately. BE original vintages are unknown; YTD cutoffs are 31 December 2025. Negative recoveries and explicit printed zeros are retained. Detailed net capital YTD of ₹228,952.36 crore differs from the narrative overview of ₹227,021 crore; the extract uses the detailed table and does not silently substitute or combine those views.

NHAI's financial statement contains 38 balance-sheet cells, 33 annual cash-flow cells and three debt balances. The current dash for NCD redemption is omitted, not converted to zero. Original **INR lakh × 0.01** gives INR crore. March 2025 total debt reconciles to secured plus unsecured borrowings and is ₹244,604.0654 crore; the March 2024 audited comparator remains ₹335,373.1966 crore at its original balance date. Cash-flow bond interest and other expenditure is separate from establishment finance cost. Adjusted toll ploughback is separate from gross toll receipts. The reported capital-road-work total includes the fixed-asset net block, so completed road capital and work in progress alone do not reproduce that total. NHAI's implementing-agency accounting is retained rather than interpreted as ordinary corporate profitability.

## Remaining release gaps

These additions do not assert that all series are current to September 2026. Safety observations still end in 2024; the annual ministry tables still stop at December 2025; and the extracted NHAI financial statement ends in March 2025. Neither a newly ended quarter nor an annual-report title proves a later audited or quarterly filing exists. Publication discovery, observation coverage and assurance remain separate in the catalog and metric coverage report. The exact PIB HTML release remains a governed manual snapshot because wrapper counters change independently of article facts; no fabricated live endpoint is used.
