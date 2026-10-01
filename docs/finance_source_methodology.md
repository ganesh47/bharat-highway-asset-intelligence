# Primary finance and highway disclosure methodology

Research cutoff: 2 October 2026. The inventory adds 16 government, issuer,
payment-system and multilateral sources. Government authority does not establish
extract quality; issuer disclosures are not classified as government measurements.

The governed snapshots contain **4,179 numerical facts**, of which **3,852** are
eligible for scoped calculations, from 15 retrieved sources.
One source has a typed empty snapshot and an explicit evidence gap. Every
nonempty row records its source URL, physical document/table/page anchor, source
SHA256, original units, normalized value, entity, road class, period, statement
basis, estimate type, publication date when disclosed, and observation date.

| Source | Extracted coverage | Important limits |
| --- | --- | --- |
| Union Budget Demand86 | 40 actual/BE/RE gross, recovery, net, NHAI, road works, CRIF, maintenance and safety allocations | Revenue/capital/total columns; transfers and recoveries are never summed into net spending; 10 reproduced prior-BE rows have unknown original vintage and remain disclosure-only |
| Outcome Framework2026-27 | 12 construction, PPP, monetisation, safety and tolling targets | Forward targets, no achievement inference |
| NHIT June2026 presentation | 117 consolidated/SPV finance, asset-group traffic and toll facts | Portfolio expansion; NSPPL FY26 ETC-only versus FY27 full collection traffic; rounded amounts |
| NHIT quarterly board outcome | 6 distribution and NAV/EV facts | Scanned AnnexureI accounting statements remain quarantined |
| NHIT June2026 valuation | 17 concession-round, portfolio and WACC facts | Valuer assumptions differ from measurements; INRmillion concession fees normalized to INRcrore |
| NHIT Annual Report2025-26 | 62 audited consolidated financial, SPV operating, debt flow and instrument maturity facts | Audited INRlakh normalized; fiscal publication day undisclosed; maturity uses contractual undiscounted basis; eight source dashes omitted |
| NHIDCL PMP31August2026 | 2,384 facts from410 detailed project rows | Termination and MMLP rows differ from summary scope; contractor role is not assumed to mean operator |
| Parliament UQ963 | 33 debt/TOT/InvIT facts | Debt asof31December2025; closed fiscal receipts retain their year-end cutoff; TOT17 and Round4 receipts are FY25-26YTD through5February2026; Round4 table year is preserved separately |
| PIB30March2026 monetisation | 6 realised/target/InvIT5/TOT18 facts | FY25-26 YTD before year end, not final actual |
| RBI State Finances2025-26 | 1,302 Roads and Bridges budget and all-sector state liabilities/guarantees facts | All road classes, not SH-only; actual/BE/RE distinct; actuals end no later than31March2024; 279 BE/RE rows have unknown state-specific vintage and remain disclosure-only; Goa revenue dashes retained as absence |
| CAG Report19of2023 | 6 Bharatmala programme/sample facts | Historical audit; sample66 cannot define all-India project failure incidence |
| MoRTH Basic Road Statistics | 75 SH network/surface facts | State footnotes2018-2021 override report headline2022; duplicate spreads excluded; inconsistent rows quarantined |
| UPEIDA project HTML | 8 route, grant and cost context facts | Verified TLS macOS curl retrieval; financial observation/publication dates undisclosed; all rows excluded from measured analytics; Agra-Lucknow cost excludes land |
| NPCI NETC statistics | 34 monthly payment-volume and amount facts, April2025–August2026 | Governed manual capture of the rendered official table; annual pass and Maharashtra Electric Vehicle exempted data excluded as stated by NPCI; no NHAI-receipts or traffic-count inference |
| MSRDC financial disclosures | Evidence gap | Index/filings time out; subsidiaryFY23-24 is not parent standaloneFY23-24 |
| ADB project52298-001 final accounts | 77 project/package finance and target facts | MPWD cash-basis accounts through19May2025;70 actuals eligible,6 targets and1 unreconciled deposit total excluded; scanned cells visually checked against exact PDF bytes |

## Financial and comparison rules

Currency normalization is INRcrore. INRlakh is multiplied by0.01; INRmillion by0.1.
INRthousand is multiplied by0.0001.
Per-unit distributions stay INR/unit. Both original value/unit and normalized
value/unit remain visible. Gross/net allocations, debt carrying values, debt
outstanding, concession proceeds, total liabilities, and enterprise values are
different metrics. An issuer InvIT debt balance is not NHAI debt.

Actual/BE/RE/YTD/target status, calendar/fiscal/quarter/stock/transaction period,
and consolidated/standalone/SPV/CFI/valuation basis form the comparison key.
Totals are cited assertions rather than additive contributions from every report.
The same InvIT round in Parliament, issuer valuation and press releases must not
be counted three times. Route-km and lane-km remain separate units.

`data_as_of` identifies the observation cutoff or an explicitly supported estimate
vintage. It never becomes a newer date merely because an older number is repeated
in a later document. `disclosure_as_of` preserves later reporting/assertion context,
`published_at` preserves publication, `estimate_vintage` identifies the dated
estimate when established, and `reported_period` preserves the source's year label.
Blank estimate vintage means unknown. The FY2024-25 Budget actuals end31March2025;
NHIT Q1FY26 comparator observations end30June2025; historical concession rows use
their stated effective dates. The current Budget edition establishes1February2026
for FY2025-26RE and FY2026-27BE, but does not establish the original vintage of its
reproduced FY2025-26BE column. RBI's23January2026 publication is not an observation
date for FY2023-24 Accounts or a state-specific BE/RE vintage. These289 unknown-vintage
estimates remain readable and are excluded from arithmetic by the existing date guard.

`disclosure_ready` means verified numerical disclosures can be shown.
`analytical_eligible` on each row controls measured calculations. Targets,
valuation estimates, undated project figures, the BRS Arunachal duplicated13500km
assertion, and its inconsistent published total193740km versus state sum193741km
have this flag false. They remain visible with their evidence/qualification.

NHIT debt maturity rows distinguish term loans, NCDs, zero-coupon bonds, and the
issuer's rounded debt-only total from all financial liabilities. FY2026 total
debt maturity overview is233.50/556.85/24248.80crore for <1/1-3/>3years.
Repayment amounts are positive outflow magnitudes; they are not debt stocks.
Eight NCD/zero-coupon bond <1year and1-3year cells are printed as dashes on
physical page205. They have no numerical facts and are not interpreted as zero.
The disclosed term-loan amounts, carrying values, >3year instrument values and
separately reported all-debt maturity overview remain available.

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

NPCI NETC uses a governed manual browser capture of the official product-statistics
table. Reporting-year selection and scrolling were checked to load all twelve
FY2025-26 months and the five available FY2026-27 months. The pinned snapshot JSON
records the rendered cells, headings and exclusions; its SHA256 identifies the
capture, not original publisher PDF or HTML bytes. September2026 was not displayed.
Month ends are observation cutoffs; the capture date is retrieval only and the
publication day remains unknown. HTTP403/JavaScript restrictions still prevent a
verified automatic fetch. New months require another reviewed snapshot.
NPCI's scheme statistics are institutional issuer disclosures; they do not
become government measurements or proxy data because of portal access restrictions.

ADB-hosted accounts for project52298-001 are borrower-owned MPWD accounts for
Loan3911-IND. Government of India is the borrower, ADB the lender and MPWD the
executing agency. The final audited current period is1April2024-19May2025,
with priorFY2023-24 and cumulative1April2020-19May2025 columns. Financial closure
is19May2025; authorisation20August2025 and auditor22August2025 are distinct from
the ADB document page's5September2025 publication. These dates do not imply a
current whole-state balance sheet or an SPV debt exposure.

The exact25-page publisher PDF was downloaded through the manual browser and its
SHA256 pinned. Embedded OCR misreads some amounts, including printed current
payments569785 as569795 and reimbursement674809 as674909, in INRthousand.
The builder reproduces visually validated source cells only for the pinned bytes;
a changed PDF requires another review. Annexures1-3 provide cash-basis payments,
financing shares and reimbursement claims. Package civil-work totals on pages11-12
reconcile to14934706 INRthousand. Nine printed contractual-deposit values on page11
sum928394 while the reported total is928530 INRthousand, a136-thousand discrepancy.
Both assertions are preserved without a balancing amount, and the reported total
is excluded from measured arithmetic. Source dashes have no numerical facts.
The450km road objective,5year maintenance objective and historical PAM cost plan
remain target context. Neither project closure nor the audit establishes measured
delivery of the planned length or maintenance. Automated ADB access remains
restricted; the reviewed download is the evidence, with no automatic refresh claim.

The nine disabled legacy manual/proxy sources retain their identifiers, raw rows,
and evidence gaps. Unsupported URLs are replaced with verified discovery links:
[NCRB ADSI](https://www.ncrb.gov.in/accidental-deaths-suicides-in-india-adsi.html),
[Parliament Questions & Answers](https://sansad.in/rs/questions/questions-and-answers),
[RBI DBIE](https://data.rbi.org.in/),
[MoSPI eSankhyiki](https://esankhyiki.mospi.gov.in/macroindicators-main),
[CPPP ePublishing](https://eprocure.gov.in/epublish//app),
[NPCI NETC ecosystem statistics](https://www.npci.org.in/product/ecosystem-statistics/netc),
[Overpass](https://overpass-api.de/), and
[NASA VNP46A4](https://ladsweb.modaps.eosdis.nasa.gov/missions-and-measurements/products/VNP46A4/).
These links establish discovery, not numerical lineage, extracted periods,
automatic access or redistribution rights. The legacy toll source's NCRB prefix
is retained for compatibility while its publisher is corrected to NPCI; it stays
separate from the verified monthly snapshot. MoRTH's arbitration SOP is policy
context, not evidence of claim amounts. No source dates are inferred from these
portals or from the years in unverified manual rows.

The two original OGD API definitions now contain concrete resource UUID URLs.
Their official resource-page metadata advertises API availability and the same
UUIDs. This verifies identification rather than successful authenticated access;
the existing credentialed API and direct-file fallback remain unchanged.

For the known PIB monetisation HTML page only, dynamic script/viewstate wrappers
change raw bytes while visible disclosure text stays identical. Its evidence stores
a durable SHA256 of all normalized visible text. A refresh re-extracts that text:
an exact semantic digest match archives the new raw wrapper and records both
checksums while preserving the CSV's pinned raw checksum and observation date.
A change in visible text still requires a new governed extract. This rule works
even when CI has only the committed evidence JSON and no restored raw HTML cache.

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

The legacy four-state Northeast project-status CSV is cross-checked against
MoRTH Rajya Sabha unstarred question210, answered27November2024, physical page1.
All45 numerical cells match, including Nagaland's explicit zero approved-project
cells. The answer date is the latest supported observation cutoff; its
FY2024-25 completed-project columns are year-to-date. They are not a March2025
full-year result. Approved-project costs describe the pipeline and are not
incurred expenditure. The cited Total covers only Arunachal Pradesh, Nagaland,
Manipur and Tripura. A committed reference-evidence JSON pins the PDF and CSV
checksums and the comparison cells.

The legacy five-state NIP project summary is planned pipeline context. Its
capital-outlay columns cover2020-2024 planned investment, not incurred expenditure
or a2024 stock observation. The original parliamentary answer date is unverified.
The official OGD resource's embedded publication timestamp gives23December2021;
its January2022 metadata update does not refresh the underlying observation.
This source retains its existing identifier and raw snapshot but is marked
`evidence_class=target` and `analytical_eligible=false`. Its observation cutoff
stays unknown. The five states are Uttar Pradesh, Tamil Nadu, Kerala, West Bengal
and Assam; this table cannot establish all-state project coverage.

The legacy47-row Jharkhand district/project table is matched to MoRTH Rajya
Sabha starred question52, answered7February2024, physical pages3-6. It combines
route length, completed length explicitly through31March2023, and FY2023-24
targets. The source cutoff uses the answer date; it is not31March2024. Its source
evidence class is `mixed_actual_target` and it remains a disclosure with
`analytical_eligible=false` until individual columns are normalized. The source
CSV's NA cells remain nonnumeric; the reference evidence preserves the original
NIL, dash, blank and Bridge Work tokens without replacing them with zero.

The legacy NHAI audited-results source retains only its verified FY2023-24
official PDF link. Its six scanned pages were retrieved and the first-page
heading visually checked: quarter/year ended31March2024, amounts in INRlakh.
Annual filename guessing and future fiscal coverage claims are removed. This
source supplies document metadata; accounting amounts await validated extraction
and do not enter measured calculations. The report's publication day is unknown.

Issuer/multilateral public disclosure is not blanket permission to redistribute
whole reports. CSV extracts link to originals and cite only relevant tables;
no blanket OGL licence is asserted for those documents.
