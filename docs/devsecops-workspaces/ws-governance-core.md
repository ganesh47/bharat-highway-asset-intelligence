# Workspace `ws-governance-core`

## Mission
Harden repository governance with explicit merge controls, ownership boundaries, and branch protections.

## Deliverables
- `docs/governance/branch-protection.md`
- `docs/governance/ruleset-baseline.json`
- `CODEOWNERS` updates

## Stable check names to export
- `Workflow YAML syntax checks`
- `Python package sanity checks`
- `Artifact validation`
- `Deploy to GitHub Pages`
- `Playwright CD smoke check`

## Merge gate checklist
- Verify `CI Quality` and `Security Supply Chain` both green.
- Validate Pages deploy + smoke from `Deploy to GitHub Pages` are passing.
- Validate evidence workflow has run at least once in the last 24 hours.
- Confirm `docs/governance/ruleset-baseline.json` unchanged without peer approval.

## Handoff contract to `ws-integration`
- If this workspace adds any required check names, communicate them in this file.
- If branch or ruleset policy changes, provide corresponding required-status-check list.
