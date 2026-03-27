# Workspace `ws-security-supplychain`

## Mission
Implement security and supply-chain controls using OSS tooling across dependencies, secrets, and static analysis.

## Deliverables
- `.github/dependabot.yml`
- `.github/workflows/reusable-security-scans.yml`
- `.github/workflows/security-supplychain.yml`

## Stable check names and thresholds
- `Dependency security review` (fail on `high` and above by default)
- `Secrets scan` (must fail on new secret findings)
- `CodeQL analysis` (must fail on actionable alerts)
- `Filesystem vulnerability scan` (informational initially; may graduate to blocking for high only)

## Handoff contract to `ws-integration`
- Publish escalation path and SLA in `docs/devsecops-workspaces/ws-integration.md`.
- Keep alerting consistent across GitHub Security alerts, workflow summaries, and issue creation.
