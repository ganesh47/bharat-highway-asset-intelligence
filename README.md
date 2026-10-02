# Bharat Highway Asset Intelligence

An evidence-based dashboard for India's National Highways, State Highways, state expressways and disclosed highway finances. The enrichment baseline uses primary government, issuer and multilateral disclosures available through **2 October 2026**, preserving each observation's own reporting date.

Open the [published dashboard](https://ganesh47.github.io/bharat-highway-asset-intelligence/). Read the [source-by-source coverage matrix](docs/coverage_matrix.md), [financial extraction methodology](docs/finance_source_methodology.md), and [upgrade and validation record](docs/governance/2026-10-02-finance-enrichment.md) before comparing figures.

## What is included

- `research/` source planning module with machine inventory and link status tracking
  - `research/source_inventory.yaml` (human editable, manual approval gate)
  - `research/source_inventory.json` (machine generated)
  - `python -m research.scan`
  - `python -m research.gap_report`
- pluginized connectors in `pipelines/connectors/`
- official-first ingestion entrypoint `python -m pipelines.ingest`
- DuckDB-WASM + React UI at `apps/web/`
- confidence scoring and citation-aware manifest generation

## Data policy

- Official-first sources are preferred (`NHAI`, `MoRTH`, `NCRB`, `data.gov.in`, `RBI`, `MOSPI`, `.gov.in` domains).
- Proxy sources are explicitly tagged as such (e.g., OpenStreetMap geometry context).
- Crawlers and scans never bypass captcha/restricted access.
- Automated fetch runs only for entries marked `allow_auto_fetch: true` in `source_inventory.yaml`.

## Setup

```bash
pyenv install -s 3.11.9
PYENV_VERSION=3.11.9 python -m venv .venv
source .venv/bin/activate
PYENV_VERSION=3.11.9 python -m pip install -r requirements.txt
```

## Research commands

```bash
PYENV_VERSION=3.11.9 python -m research.scan          # validates inventory entries and writes status fields
PYENV_VERSION=3.11.9 python -m research.gap_report    # writes research/gaps.md
```

## Run ingestion

```bash
PYENV_VERSION=3.11.9 python -m pipelines.ingest
```

### Build analysis artifacts

```bash
PYENV_VERSION=3.11.9 python -m pipelines.correlation
```

Notes:
- The scaffold now includes all source connectors in the connector registry, including stub connectors for restricted/proxy sources.
- `python3 -m pipelines.ingest` runs all inventory sources.
- For immediate local execution without API key, place a CSV in:
  - `data/raw/manual/data_gov_in_nhai_projects_api.csv`
  - `data/raw/manual/data_gov_in_nhai_project_finance_api.csv`
  - `data/raw/manual/morh_contractor_disclosures.csv`
  - `data/raw/manual/morh_arbitration_claims.csv`
  - `data/raw/manual/morh_procurement_awards.csv`
  - `data/raw/manual/parliament_qa_highway_queries.csv`
  - `data/raw/manual/morth_annual_report_pdf.csv`
  - `data/raw/manual/ncrb_toll_fastag_claims.csv`
  - `data/raw/manual/quality_maintenance_indicators.csv`
  - `data/raw/manual/rbi_mospi_macro_indicators.csv`
  - `data/raw/manual/viirs_nightlights_proxy.csv`
- For authenticated API fetch, set:
  - `DATAGOVIN_NHAI_RESOURCE_ID`
  - `DATAGOVIN_API_KEY`

To regenerate research artifacts:

```bash
PYENV_VERSION=3.11.9 python -m research.scan
PYENV_VERSION=3.11.9 python -m pipelines.ingest
PYENV_VERSION=3.11.9 python -m pipelines.correlation
PYENV_VERSION=3.11.9 python -m research.scan --sync-catalog
PYENV_VERSION=3.11.9 python -m research.gap_report
PYENV_VERSION=3.11.9 python scripts/validate_artifacts.py --inventory research/source_inventory.yaml --catalog data/manifests/catalog.json --manifests data/manifests --fail-on-warning
PYENV_VERSION=3.11.9 python scripts/build_coverage_report.py
```

## Frontend

```bash
python3 -m http.server 4173 --directory .
# open http://localhost:4173/apps/web/index.html
```

`apps/web/index.html` loads `apps/web/src/app.js`, queries `data/manifests/catalog.json` and parquet files via DuckDB-WASM.

### GitHub Pages (static deployment)

This repository uses branch-based GitHub Pages publishing, not GitHub's Pages workflow-artifact actions. The site is static: `apps/web` + DuckDB-WASM + generated parquet/manifests copied into the published bundle.

Workflow: `.github/workflows/github-pages.yml`

Required repository Pages mode:
- deploy mode / API `build_type`: `legacy`
- source branch: `gh-pages`
- source path: `/`

Required secret:
- `PAGES_DEPLOY_TOKEN`
  - used only by the deploy workflow to verify Pages configuration, read the published URL, and push the packaged site to `gh-pages`

Why branch deploy is used:
- the repository previously hit upstream Node runtime warnings in GitHub-maintained Pages actions
- the current deploy path avoids those actions and publishes directly to `gh-pages`

On a push to `main`, the research workflow:
- runs scan + gap report
- runs ingestion and correlation generation
- runs the NHAI OCR shard/merge/confidence-refresh path only when OCR-relevant pipeline, source, manifest, or workflow files change
- stages downloads and preserves the last validated observation after failures
- validates artifacts and publishes the `bhai-research-artifacts` Actions artifact

Daily scheduled and manual runs use the same pipeline. A manual run can select `ocr_mode=skip` to refresh other sources without repeating OCR; existing extraction quality remains tied to its exact source checksum. Scheduled runs exercise the full OCR path.

After a successful trusted research run on `main`, the deployment workflow restores **that run's validated artifact**, packages `apps/web` with its manifests and parquet files, and publishes to `gh-pages`. The bundle manifest records the research run ID, source revision and catalog checksum. Post-deployment checks verify that the published catalog matches the artifact before running Playwright chart, evidence-filter, citation, accessibility and screenshot checks.

Manual run:

1. In repository settings, ensure **Pages** is configured to deploy from a branch:
   - branch: `gh-pages`
   - folder: `/ (root)`
2. Confirm repo Pages API state is still:
   - `build_type = legacy`
   - `source.branch = gh-pages`
   - `source.path = /`
3. Ensure repo secret `PAGES_DEPLOY_TOKEN` is configured.
4. Push to `main` or dispatch `research-pipeline.yml` from `main`. Direct manual deployment requires a successful research artifact for the same revision.
5. Access at `https://<org-or-user>.github.io/<repo>/`.

## Output artifacts

- Raw files + checksums in `data/manifests/<source_id>.json`
- Parquet tables in `data/processed/<source_id>.parquet`
- Unified dataset index `data/manifests/catalog.json`
- Confidence badges and citation fields in each manifest row

## Analytical boundaries

The default dashboard uses validated observations. Synthetic demonstrations, document-presence indexes and unsupported manual records remain available for review and are excluded from measured totals. Coverage counts distinguish source discovery from extracted analytical evidence.

Financial records carry entity and agency identifiers, road class, metric, original and normalized units, reporting period, estimate class, statement basis, observation cutoff, source document checksum and page/table citation. Money is normalized to ₹ crore; ₹ per unit, transaction counts, PCU traffic, route-km, lane-km and bridge counts retain their separate units. Issuer disclosures are classified separately from government statistics.

Actual expenditure, budget estimates and revised estimates are separate observations. NHIT trust/SPV accounts, NHAI standalone disclosures, ministry budgets and state Roads and Bridges expenditure have different entity and expenditure boundaries. NPCI NETC amounts represent payment-system activity and cannot substitute for NHAI receipts or corridor traffic. Valuation assumptions, delivery targets and historical audit samples have their own evidence classes.

Network stocks, construction flows and project portfolios are separate measures. State-specific older dates and published discrepancies in Basic Road Statistics remain visible. Cost/km requires compatible length-based works, with lane, mode and land-cost scope disclosed. Exploratory correlations require an approved comparable pair and at least ten matched observations; sample size accompanies every result.

## Safe refresh and source evidence

`pipelines.ingest` stages each connector output and validates its structure, semantics and provenance before publication. A failed fetch or changed source document requiring re-extraction retains the last validated parquet and its observation dates. `last_checked_at` records the attempt; it does not improve observation freshness. The full audit appears in `data/manifests/refresh_report.json` and the generated coverage matrix.

Governed manual extracts require a source-specific CSV plus `data/raw/manual/evidence/<source_id>.json`, containing identifiable primary documents, their checksums and extraction metadata. Unsupported manual records are quarantined. Accessible primary documents are checked within their configured fetch policy; restricted publishers remain manual gaps. Publisher-specific terms apply; the India OGD license is not inherited by unrelated publishers.

After ingestion, regenerate the accountability report with:

```bash
python scripts/build_coverage_report.py
```

## Publication coverage at 2 October 2026

The reconciled inventory has **62 sources** and **63 datasets**. Governed primary snapshots contain **10,640 numerical facts**, including **10,262 calculation-eligible observations**; these heterogeneous facts are not an additive network or project total.

New verified evidence includes final 2024 road-safety tables, FY2024–25 GSDP, audited FY2024–25 Roads and Bridges spending for five states, complete FY2025–26 NHAI funding and separate all-NH construction, and August 2026 costs/progress for 982 PAIMANA-monitored highway projects. NHIT remains at June 2026 and NETC/NHIDCL at August 2026 where later observations were not verified. Missing September-quarter releases are explicit gaps.

The evidence explorer separates observation status, assurance, road class, entity and reporting period, and supports all-history, latest-per-entity and common-period views. Metric-level coverage distinguishes observation dates, publication lag, check outcomes and announced next releases. Old source vintages remain accessible; incompatible periods and network/project scopes are not combined.

See the [state publication audit](research/state_publication_audit_2026_10_02.md), [national extraction methodology](docs/national_freshness_methodology.md), and [freshness implementation notes](docs/freshness_implementation_notes.md).
