# Dependency automation

This repo uses a split automation model:

- **GitHub-native** for detection, CI, and gated merge behavior
- **grclaw local sandbox** for Codex-assisted remediation of failing dependency PRs

## GitHub workflows

- `.github/workflows/dependency-remediation.yml`
  - scheduled remediation PR generation when open Dependabot alerts exist
- `.github/workflows/dependency-automerge.yml`
  - enables auto-merge for clean dependency PRs only

## Local grclaw Codex loop

Script:

- `scripts/dependency_pr_codex_loop.sh`

What it does:

1. Finds open failing dependency PRs created by Dependabot
2. Checks out the PR branch locally
3. Runs Codex in the grclaw sandbox host with a minimal-fix prompt
4. Runs local validation
5. Pushes back to the PR branch only if validation passes
6. Comments on the PR with the result

## Required local tools

- `gh`
- `git`
- `jq`
- `python3`
- `codex`

## Suggested OpenClaw cron setup

Because cron registration depends on the local paired gateway, register this from the grclaw host after pairing is available.

Suggested stable job name:

- `repo:dependency-pr-codex-loop`

Suggested command pattern:

```bash
cd /home/ganesh/.openclaw/workspace/bharat-highway-asset-intelligence && ./scripts/dependency_pr_codex_loop.sh
```

Suggested cadence:

- every weekday, after Dependabot and CI have had time to run

## Safety model

- auto-merge is limited to dependency PRs that are already clean
- Codex only targets failing dependency PRs
- Codex must pass local validation before pushing
- GitHub required checks remain the final gate before merge
