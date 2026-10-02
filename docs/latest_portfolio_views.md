# Latest analyst and monitored-portfolio views — 2 October 2026

## Baseline captured before frontend edits

The published dashboard at `https://ganesh47.github.io/bharat-highway-asset-intelligence/apps/web/` was captured on 2 October 2026 at 08:05:58 UTC. The source baseline was release `6864446c4666e0b8e281b1d7f2dc7995bbb3dde9`: 62 registered sources, 63 datasets, and the established typed primary-disclosure schema. Source identifiers and primary URLs remain in the source inventory and governed document/fact bindings; this frontend change does not fetch or rewrite source evidence.

The baseline's analyst period view was `all`. The state project mix used the March 2024 NH annexure and fed the delayed-project bubble and delay/GSDP comparison. The economic chart chose FY2022–23 for 34 current geographies, plus the separately scoped historical Jammu and Kashmir observation. Baseline evidence is saved in ignored QA files `buildcheck/latest-default-before.json`, `latest-default-before-desktop.png`, `latest-default-before-mobile.png`, and readable affected-card screenshots `latest-default-before-project-mix.png`, `latest-default-before-economic.png`, and `latest-default-before-delay.png`.

## Defaults and historical evidence

The analyst table defaults to latest available per entity within compatible metric, agency, road class, unit, accounting basis, period basis, estimate, observation-status and assurance scopes. Different scopes can have different periods. The selection summary names reporting periods and observation cutoffs, and reports the filtered historical or undated observations retained under All history. A later disclosure date is never substituted for an observation cutoff. Unknown estimate vintages cannot establish a latest observation; their supported evidence remains available in All history with its qualifiers.

All history and Common comparable period remain explicit modes. The common mode retains the existing widest-coverage rule among the latest three periods within each compatible scope. NETC trends retain the filtered monthly history independently of the table's period mode; agency, reporting-period, metric and evidence filters still apply. Latest table rows and historical trend rows are not silently combined into a growth calculation. CSV exports retain the selected table rows and original units, cutoff, disclosure date, estimate vintage, period label, assurance, revision identity and document checksum.

## Project and economic comparisons

`nhidcl_monthly_project_progress` supports a latest monthly portfolio of distinct project IDs. Only analytically eligible NHIDCL National Highway facts with project-snapshot/PMP Data Lake scope contribute. Multiple cost, length and progress facts for one ID count as one project. Conflicting location, stage or completion-date descriptors exclude the project rather than choosing a row arbitrarily. The source's reported stage governs Completed, Ongoing and Other reported stages.

At the inspected 31 August 2026 snapshot there are 406 distinct projects across 13 disclosed location labels: 214 reported completed, 144 ongoing, and 48 in other reported stages. Of the ongoing projects, 92 have a valid reported scheduled completion before the snapshot and 52 are not past that reported schedule; none in this extract has an unknown ongoing schedule. The code retains an explicit unknown-schedule category. Invalid or ambiguous dates are unknown, never a deadline.

Schedule exposure is a comparison with the reported schedule, not a reported/adjudicated contractual delay or a finding about contractor liability. Original versus extended completion and approved extension-of-time records are not resolved. The portfolio is not all NHAI, MoRTH or State Highway works. The March 2024 NH annexure remains separately dated historical context with its primary citation; its definition is not spliced into NHIDCL.

The economic scatter uses one latest published fiscal year from `rbi_gsdp_current_prices_2024_25`, with current-price INR-crore values and current state-aggregate geographies. FY2024–25 has 25 current state/UT observations in the baseline vintage. Missing states and historical geographies are excluded; ambiguous duplicate/revision cells are not selected by row order. The network axis uses actual MoRTH state NH stock at 31 December 2025 from `morth_annual_report_2025_26`, not portfolio works length. The unmatched published national total remains excluded. The separately labelled 2024 Appendix charts and safety denominator retain their own vintages.

Bubble size is the distinct NHIDCL portfolio project count. A fixed marker outside the portfolio means coverage unavailable, not zero projects. The schedule-exposure ranking divides known past-schedule ongoing project counts by same-state FY2024–25 GSDP in INR crore and scales by 100,000. Seven portfolio locations have a compatible latest GSDP denominator; six are omitted. This is context across separately dated observations, not a current-year delivery rate or causal measure. Unknown schedules are disclosed alongside the known-schedule count.

## Remaining scope gaps

- No complete recent whole-NHAI or all-State-Highway project census is inferred from the NHIDCL extract.
- PAIMANA monitors projects of at least ₹150 crore and overlaps agency portfolios. Its IDs have no verified crosswalk to NHIDCL, and its typed rows do not disclose exact completion dates suitable for this schedule calculation. The portfolios are not added together.
- Approved schedule revisions, extensions of time and consistent contractual delay definitions are not available for every monitored project.
- The latest GSDP vintage does not disclose FY2024–25 for every current jurisdiction. Older state years remain in analyst history and the coverage table; they are not mixed into the latest-period scatter.
- December 2025 NH stock and August 2026 project observations have different cutoffs from FY2024–25 GSDP. A newer network stock does not supply a missing same-period safety denominator or vehicle-exposure denominator.
- These frontend changes do not close state-account extraction or publisher-release gaps. The source/state coverage matrix and publication discovery retain the integration team's evidence outcomes.

## Verification and release

The existing 13 government-chart requirements remain covered, with the project/delay semantic checks updated to the explicitly scoped monitored portfolio. Fixtures cover deduplication, invalid/unknown schedule dates, reported completed versus ongoing stages, snapshot boundaries, conflicts, exclusion of MMLP/other agencies, one-period GSDP selection, incompatible network stocks, missing denominators and observed zero exposure. Browser checks cover latest defaults, retained historical/unknown-vintage estimates, 17-month NETC history, CSV lineage, citations, chart labels/units/dates, keyboard readouts, and 390-pixel overflow. Horizontal zero bars have zero width instead of a fabricated minimum fill.

Local browser evidence is saved under `buildcheck/latest-default-after-*` and `buildcheck/latest-default-smoke.log`. Final integration must pass GitHub Actions, deployment byte/provenance verification and post-CD browser checks against the resulting published revision; local results alone do not establish a deployed release.
