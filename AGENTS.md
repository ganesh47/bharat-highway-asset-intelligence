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
