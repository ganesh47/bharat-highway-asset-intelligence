# Workspace `ws-observability-evidence`

## Mission
Generate evidence artifacts and SLO-driven reports that can be audited per release.

## Deliverables
- `scripts/devsecops_evidence.py`
- `.github/workflows/devsecops-observability.yml`

## Evidence artifacts
- Markdown + JSON runbook snapshots in workflow artifacts:
  - latest workflow status for required pipelines
  - latest check runs for the triggering commit
  - deployment URL + bundle fingerprint when available

## Retention
- Evidence artifacts retained in GitHub Actions artifact store for at least 90 days (repo policy).
- Long-term summary artifacts can be moved to release notes as needed.

## Handoff contract to `ws-integration`
- Provide the exact artifact filename and check-name map used by each evidence run.
- Report missing/inconclusive checks as explicit `"missing"` status values.
