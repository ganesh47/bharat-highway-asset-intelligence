# Workspace `ws-governance-core`: Branch & process controls

This repository uses public GitHub-native controls as merge gates for all `main` updates.

## Required status checks

- `CI Quality` (workflow result)
- `Security Supply Chain` (workflow result)
- `Deploy to GitHub Pages` (deploy and smoke checks must be green)
- `DevSecOps Observability and Evidence` (evidence generation job)

The following job names are stable and should remain unchanged for governance automation:

- `Workflow YAML syntax checks`
- `Python package sanity checks`
- `Artifact validation`
- `Dependency security review`
- `Secrets scan`
- `CodeQL analysis`
- `Build`
- `Deploy`
- `Playwright CD smoke check`
- `Evidence publication`

## Ruleset baseline

Apply the policies below with repo rulesets (or branch protection rules as fallback on the branch `main`):

1. Require pull requests and up-to-date branch for `main`.
2. Require status checks listed above.
3. Require at least one approval on code changes to:
   - `/.github/`
   - `/.github/workflows/`
   - `/scripts/`
   - `/docs/governance/`
4. Block force pushes and deletions on `main`.
5. Restrict workflow modifications to the governance/security maintainer group.
6. Require signed commits where tooling and permissions permit.
7. Enable linear history for the `main` branch.

## One-command guardrail (post-change)

Use `gh` checks before enabling/altering rules:

```bash
gh api repos/:owner/:repo/rulesets
```

`docs/governance/ruleset-baseline.json` is the source-of-truth draft that can be used to provision or validate these rules.

## Merge gate matrix

| File scope | Required check |
| --- | --- |
| `.github/workflows/**` | CI Quality + Security Supply Chain |
| `scripts/**`, `pipelines/**` | CI Quality |
| `docs/governance/**`, `docs/devsecops-workspaces/**` | CI Quality + Security Supply Chain |
| `apps/web/**` | CI Quality + Deploy to GitHub Pages |
