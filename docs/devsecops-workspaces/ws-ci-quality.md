# Workspace `ws-ci-quality`

## Mission
Build deterministic CI quality gates and reusable checks for data contracts, workflow syntax, and repository validation.

## Deliverables
- `.github/workflows/reusable-ci-quality.yml`
- `.github/workflows/ci-quality.yml`

## Stable check names and semantics
- **Must-pass checks**
  - `Workflow YAML syntax checks`
  - `Python package sanity checks`
  - `Artifact validation`
- **Informational checks**
  - `Quality gate status`

## Workspace rules
- Keep checks deterministic and offline where possible.
- Never block `main` on flaky or external-latency-only checks by using `continue-on-error` only on explicitly documented informational jobs.
- Keep required status check names stable for integration/gates.

## Handoff contract to `ws-integration`
- Provide workflow names and job IDs as the truth source.
- Include failure rationale if optional checks are allowed to continue-on-error.
