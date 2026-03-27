# Workspace `ws-integration`

## Mission
Coordinate handoff, conflict resolution, and final merge ordering across independent workspaces.

## Thread dependency graph
- `ws-governance-core` publishes required check names and branch policy contract.
- `ws-ci-quality` provides lint/test contract checks and reusable workflow.
- `ws-security-supplychain` provides security job contract and dependency policy baseline.
- `ws-cd-provenance` consumes CI contracts and emits stable deploy outputs.
- `ws-observability-evidence` consumes outputs from all workspaces to produce release evidence.

## Merge order
1. `ws-governance-core`
2. `ws-ci-quality`
3. `ws-security-supplychain`
4. `ws-cd-provenance`
5. `ws-observability-evidence`
6. `ws-integration` (final reconciliation only)

# Go/No-Go gate list
- All workspace contracts documented in their markdown files.
- Required checks green:
  - `CI Quality`
  - `Security Supply Chain`
  - `Deploy to GitHub Pages`
  - `DevSecOps Observability and Evidence`
- Required workflow outputs present:
  - `deploy.outputs.page_url`
  - `deploy.outputs.bundle_sha256`
- Evidence artifact generated at least once in last pipeline cycle.
