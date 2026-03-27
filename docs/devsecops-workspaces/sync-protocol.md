# DevSecOps Workspace Sync Protocol

All workspace threads should emit one short sync artifact entry per completed work cycle with:

- changed files
- changed check names
- new/changed required-contract values
- risks and follow-up actions

Suggested command for each thread:

```text
docs/devsecops-workspaces/<workspace>.md
```

The `ws-integration` thread collects and reconciles these files before merge.
