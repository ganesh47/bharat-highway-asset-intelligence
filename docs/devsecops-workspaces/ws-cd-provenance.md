# Workspace `ws-cd-provenance`

## Mission
Improve CI/CD deployment safety for deterministic artifacts, provenance data, and rollback readiness.

## Deliverables
- `.github/workflows/github-pages.yml` (job outputs and bundle manifest)

## Provenance outputs
- `deploy.outputs.page_url`
- `deploy.outputs.bundle_sha256`
- `build.outputs.bundle_sha256`

## Rollback strategy
- Known-good marker: latest successful `Deploy to GitHub Pages` commit hash.
- Recovery path:
  1. Re-run `workflow_dispatch` on `Deploy to GitHub Pages` for previous successful `GITHUB_SHA`.
  2. If unavailable, restore last validated `/gh-pages` branch snapshot manually.
- Block rollback until `Deploy to GitHub Pages` and `Playwright CD smoke check` both pass.

## Handoff contract to `ws-integration`
- Keep `page_url` schema stable across deployments:
  - workflow output path remains `deploy.outputs.page_url`.
- Include deterministic bundle fingerprinting in the deployment artifact.
