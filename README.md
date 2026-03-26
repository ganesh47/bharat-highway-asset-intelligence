# Bharat Highway Asset Intelligence

This scaffold implements a research-first, official-first pipeline for official Indian transportation data.

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
PYENV_VERSION=3.11.9 python -m research.gap_report
PYENV_VERSION=3.11.9 python scripts/validate_artifacts.py --inventory research/source_inventory.yaml --catalog data/manifests/catalog.json --manifests data/manifests
PYENV_VERSION=3.11.9 python -m pipelines.ingest
PYENV_VERSION=3.11.9 python -m pipelines.correlation
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

On each push to `main`, the workflow:
- runs scan + gap report
- runs ingestion and correlation generation
- runs the NHAI OCR shard/merge/confidence-refresh path only when OCR-relevant pipeline, source, manifest, or workflow files change
- packages `apps/web` with `data/manifests` and `data/processed`
- force-pushes the packaged site to `gh-pages`
- runs Playwright smoke against the final published Pages URL, with retries to absorb GitHub Pages propagation lag

Manual run:

1. In repository settings, ensure **Pages** is configured to deploy from a branch:
   - branch: `gh-pages`
   - folder: `/ (root)`
2. Confirm repo Pages API state is still:
   - `build_type = legacy`
   - `source.branch = gh-pages`
   - `source.path = /`
3. Ensure repo secret `PAGES_DEPLOY_TOKEN` is configured.
4. Push to `main` or run `workflow_dispatch` from `main`.
5. Access at `https://<org-or-user>.github.io/<repo>/`.

## Output artifacts

- Raw files + checksums in `data/manifests/<source_id>.json`
- Parquet tables in `data/processed/<source_id>.parquet`
- Unified dataset index `data/manifests/catalog.json`
- Confidence badges and citation fields in each manifest row
