# CodeQL Remediation Notes

Date: `2026-03-31`

## Baseline Context

- GitHub live alert enumeration was attempted first, but `gh` is not authenticated in this workspace, so repository code-scanning alerts could not be queried directly from GitHub.
- Current source inventory baseline: `research/source_inventory.yaml` is `version: 1`, `last_updated: 2026-03-11`, with official-first entries spanning `data.gov.in`, `nhai.gov.in`, and related transport datasets.
- Current catalog baseline: `data/manifests/catalog.json` is generated at `2026-03-26T15:43:51.277657+00:00` and exposes citation-bearing dataset manifests consumed by the web app.
- Active pipeline baseline from repo docs: `research.scan`, `research.gap_report`, `pipelines.ingest`, `pipelines.correlation`, and `scripts/validate_artifacts.py`.
- Website baseline affected by this task: no chart, route, mode, or visual-default changes are planned. The current site is the static `apps/web/index.html` shell backed by `apps/web/src/app.js`, DuckDB-WASM asset loading, and `data/manifests/catalog.json`.

## Change Intent

- Preserve all data lineage and catalog behavior.
- Only harden outbound HTTP request paths that are driven by inventory/config values so CodeQL-style server-side request forgery findings are mitigated without changing dataset semantics.

## Live CodeQL Alerts

- Alert `#10`: `actions/cache-poisoning/poisonable-step` in `.github/workflows/github-pages.yml`
- Alert `#8`: `actions/missing-workflow-permissions` in `.github/workflows/reusable-ci-quality.yml`
- Alert `#7`: `actions/missing-workflow-permissions` in `.github/workflows/ci-quality.yml`
- Alert `#6`: `actions/cache-poisoning/poisonable-step` in `.github/workflows/github-pages.yml`
- Alert `#4`: `py/incomplete-url-substring-sanitization` in `pipelines/connectors/datagovin_ogd.py`
- Alert `#3`: `py/incomplete-url-substring-sanitization` in `pipelines/connectors/datagovin_ogd.py`

## Quality Guardrail Note

- No ontology mappings, citation fields, chart labels, units, dates, or accessibility cues are changed by this remediation.
- No source references are added; only destination host validation is enforced before network fetches.
