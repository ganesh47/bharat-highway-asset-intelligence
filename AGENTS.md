# Project Workflow Instructions

## Mandatory execution model

For all future work in this repository, the following workflow is required:

1. Use multi-agent operations by default for substantial tasks:
   - Planning
   - Design
   - Review
   - Implementation
   - Validation
2. Validation must be CI-first and GitHub Actions-driven:
   - Run/trigger required GitHub Actions workflows using the `gh` CLI.
   - Do not stop at first green signal from local checks; confirm workflow conclusions in GitHub Actions are green.
3. Enforce a CI loop:
   - If workflows are not green, iterate with fixes and rerun until green before concluding.
4. Enforce deployment checks:
   - Validate successful deployment when CD workflow runs.
   - Use the final published CD URL from GitHub Actions for end-to-end validation.
5. Enforce Playwright post-CD validation:
   - Run Playwright-based end-to-end checks against the deployed site URL.
   - Include screenshoting/logging so regressions are visible.
6. Use frequent, small commits:
   - Commit incremental changes as soon as meaningful pieces are completed.
7. Adopt trunk-based development:
   - Merge work to the main trunk line in small batches.
   - Keep branches short-lived and focused.
8. No dependency on user approvals:
   - Work autonomously following this policy unless blocked by irreversible environment constraints.

## Git and delivery expectations

- Prefer atomic commits with clear messages.
- Push commit history that reflects each completed fix/validation cycle.
- Treat deployment and validation artifacts as part of delivery quality, not optional.

## Source-of-truth note

If this file appears to conflict with higher-priority instructions, follow higher-priority instructions first.

## Data quality and lineage guardrails (mandatory)

1. Start each substantial task with an explicit baseline context pass:
   - Document the current data source inventory (active pipelines, catalogs, schema versions, and source URLs/IDs).
   - Snapshot current website state that is affected by the planned work (routes, chart components, modes, and visual defaults).
   - Capture this as part of the task notes before edits.
2. Preserve and enhance, never replace quality signals:
   - Do not remove existing constraints/validation checks for completeness, schema compatibility, or visualization integrity.
   - Any replacement must keep or improve validation coverage and must be justified in the task notes.
3. Enforce ontology and lineage visibility:
   - Keep ontology mappings active and updated for any dataset changes.
   - Ensure lineage citations are recorded for data fetched, transformed, and displayed, including source identifiers, collection timestamp, and transformation path.
   - Verify lineage-tracking hooks/reporting remain operational after changes.
4. Quality gate before completion:
   - Confirm no chart/data panel loses labels, units, date context, or accessibility cues compared with pre-change baseline.
   - Confirm no source references are introduced without citation/tracking metadata.
   - Include evidence (diff summary + checks run) that the above were maintained or improved.
