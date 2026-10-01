# Primary finance and highway disclosure methodology

Research cutoff: 2 October 2026. The inventory adds 16 government, issuer,
payment-system and multilateral sources. Government authority does not establish
extract quality; issuer disclosures are not classified as government measurements.

The governed snapshots contain **4,076 numerical facts** from 13 retrieved sources.
Three sources have typed empty snapshots and explicit evidence gaps. Every
nonempty row records its source URL, physical document/table/page anchor, source
SHA256, original units, normalized value, entity, road class, period, statement
basis, estimate type, publication date when disclosed, and observation date.

| Source | Extracted coverage | Important limits |
| --- | --- | --- |
| Union Budget Demand86 | 40 actual/BE/RE gross, recovery, net, NHAI, road works, CRIF, maintenance and safety allocations | Revenue/capital/total columns; transfers and recoveries are never summed into net spending |
| Outcome Framework2026-27 | 12 construction, PPP, monetisation, safety and tolling targets | Forward targets, no achievement inference |
| NHIT June2026 presentation | 117 consolidated/SPV finance, asset-group traffic and toll facts | Portfolio expansion; NSPPL FY26 ETC-only versus FY27 full collection traffic; rounded amounts |
| NHIT quarterly board outcome | 6 distribution and NAV/EV facts | Scanned AnnexureI accounting statements remain quarantined |
| NHIT June2026 valuation | 17 concession-round, portfolio and WACC facts | Valuer assumptions differ from measurements; INRmillion concession fees normalized to INRcrore |
| NHIT Annual Report2025-26 | 70 audited consolidated financial, SPV operating, debt flow and instrument maturity facts | Audited INRlakh normalized; fiscal publication day undisclosed; maturity uses contractual undiscounted basis |
| NHIDCL PMP31August2026 | 2,384 facts from410 detailed project rows | Termination and MMLP rows differ from summary scope; contractor role is not assumed to mean operator |
| Parliament UQ963 | 33 debt/TOT/InvIT facts | Debt asof31December2025; proceeds deposited CFI; rounded transaction assertions differ from issuer fees |
| PIB30March2026 monetisation | 6 realised/target/InvIT5/TOT18 facts | FY25-26 YTD before year end, not final actual |
| RBI State Finances2025-26 | 1,302 Roads and Bridges budget and all-sector state liabilities/guarantees facts | All road classes, not SH-only; actual/BE/RE distinct; Goa revenue dashes retained as absence |
| CAG Report19of2023 | 6 Bharatmala programme/sample facts | Historical audit; sample66 cannot define all-India project failure incidence |
| MoRTH Basic Road Statistics | 75 SH network/surface facts | State footnotes2018-2021 override report headline2022; duplicate spreads excluded; inconsistent rows quarantined |
| UPEIDA project HTML | 8 route, grant and cost context facts | Verified TLS macOS curl retrieval; financial observation/publication dates undisclosed; all rows excluded from measured analytics; Agra-Lucknow cost excludes land |
| NPCI NETC statistics | Evidence gap | Primary page403/JS restriction; no invented API or search-result observations |
| MSRDC financial disclosures | Evidence gap | Index/filings time out; subsidiaryFY23-24 is not parent standaloneFY23-24 |
| ADB project52298-001 | Evidence gap | Primary page403; discovered audit links alone do not establish numeric facts |

## Financial and comparison rules

Currency normalization is INRcrore. INRlakh is multiplied by0.01; INRmillion by0.1.
Per-unit distributions stay INR/unit. Both original value/unit and normalized
value/unit remain visible. Gross/net allocations, debt carrying values, debt
outstanding, concession proceeds, total liabilities, and enterprise values are
different metrics. An issuer InvIT debt balance is not NHAI debt.

Actual/BE/RE/YTD/target status, calendar/fiscal/quarter/stock/transaction period,
and consolidated/standalone/SPV/CFI/valuation basis form the comparison key.
Totals are cited assertions rather than additive contributions from every report.
The same InvIT round in Parliament, issuer valuation and press releases must not
be counted three times. Route-km and lane-km remain separate units.

`disclosure_ready` means verified numerical disclosures can be shown.
`analytical_eligible` on each row controls measured calculations. Targets,
valuation estimates, undated project figures, the BRS Arunachal duplicated13500km
assertion, and its inconsistent published total193740km versus state sum193741km
have this flag false. They remain visible with their evidence/qualification.

NHIT debt maturity rows distinguish term loans, NCDs, zero-coupon bonds, and the
issuer's rounded debt-only total from all financial liabilities. FY2026 total
debt maturity overview is233.50/556.85/24248.80crore for <1/1-3/>3years.
Repayment amounts are positive outflow magnitudes; they are not debt stocks.

Entity roles are additive fields: asset owner, implementing agency, operator,
concessionaire, financing entity and contractor. Document-supported NHIDCL
implementation/contractor and NHIT financing/SPV-concession roles are populated;
undisclosed roles remain blank. No company-name similarity joins these roles.

## Evidence archives and refresh

Pinned raw documents live under
`data/raw/primary_disclosures/<source_id>/document.pdf` (or`.html`). The second
UPEIDA page is`agra-lucknow.html`. Bulk PDFs are excluded from Git and restored
through the pipeline cache/artifact archive. Governed CSVs and evidence JSON are
committed under`data/raw/manual/` and`data/raw/manual/evidence/`.

The connector verifies CSV SHA256, document SHA256 if restored, required schema,
numeric finiteness, normalized units, scoped uniqueness, dates and eligibility.
Accessible automatic sources receive daily bounded TLS-verified document checks.
Candidate bytes cannot replace pinned source bytes: changed documents are saved
under`quarantine/<sha256>.pdf`, flagged for extraction, and the pipeline retains
the previous validated artifact. HTTP errors do not become successful extraction.
Manual/restricted sources receive no guessed endpoint or silent numeric fallback.

Rebuild after downloading/restoring the pinned source documents:

```sh
.venv/bin/python -m pipelines.connectors.primary_disclosures --raw-root data/raw
.venv/bin/python -m unittest tests.test_primary_disclosures -v
```

Extraction contracts intentionally fail on changed table layouts. The builder
uses PDF text and pdfplumber table extraction, and difficult NHIT traffic/annual
financial tables were rendered with Poppler and visually checked. It does not
promote unvalidated OCR from quarterly scanned accounting pages.

The existing NHAI construction series is updated to FY2025-26 final5313km,
PIB1April2026, with the prior October2025YTD3468km retained in a separate
observation history ledger. The old and final observations cover different periods;
the historical YTD value is not treated as a revision of the March full-year value.
Existing OGD financial spending remains explicitly through31January2025.

Issuer/multilateral public disclosure is not blanket permission to redistribute
whole reports. CSV extracts link to originals and cite only relevant tables;
no blanket OGL licence is asserted for those documents.
