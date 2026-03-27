#!/usr/bin/env python3
"""Emit periodic DevSecOps evidence artifacts."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
from pathlib import Path


def run_gh_json(*command: str):
    try:
        output = subprocess.check_output(["gh", *command], text=True).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None

    if not output:
        return None

    try:
        return json.loads(output)
    except json.JSONDecodeError:
        return None


def latest_workflow_run(workflow: str, branch: str = "main"):
    data = run_gh_json(
        "run",
        "list",
        "--workflow",
        workflow,
        "--branch",
        branch,
        "--limit",
        "1",
        "--json",
        "status,conclusion,headSha,event,createdAt,updatedAt,url,name,headBranch,runNumber",
        "-q",
        "[0]",
    )
    if isinstance(data, dict):
        return data
    if isinstance(data, list) and data:
        return data[0]
    return None


def latest_commit_checks(sha: str | None, repository: str):
    if not sha:
        return None

    payload = run_gh_json("api", f"repos/{repository}/commits/{sha}/check-runs")
    if payload is None:
        return []

    runs = payload.get("check_runs", [])
    return runs


def evaluate_required_checks(check_runs, required):
    statuses = {}
    by_name = {row.get("name"): row for row in check_runs or []}
    for check in required:
        match = by_name.get(check)
        if match is None:
            statuses[check] = "missing"
        else:
            statuses[check] = match.get("conclusion") or match.get("status")
    return statuses


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="buildcheck")
    parser.add_argument(
        "--workflows",
        default="CI Quality,Security Supply Chain,Deploy to GitHub Pages",
    )
    parser.add_argument(
        "--required-checks",
        default="CI Quality,Security Supply Chain,Deploy to GitHub Pages",
    )
    parser.add_argument("--branch", default="main")
    args = parser.parse_args()

    repository = os.environ.get("GITHUB_REPOSITORY", "")
    workflow_names = [name.strip() for name in args.workflows.split(",") if name.strip()]
    required_checks = [name.strip() for name in args.required_checks.split(",") if name.strip()]
    triggering_sha = os.environ.get("GITHUB_SHA")
    page_url = os.environ.get("PAGE_URL")
    if not page_url and repository:
        page_url_data = run_gh_json("api", f"repos/{repository}/pages", "--jq", ".html_url")
        page_url = page_url_data if isinstance(page_url_data, str) else None

    workflow_runs = {
        name: latest_workflow_run(name, branch=args.branch) for name in workflow_names
    }
    check_runs = latest_commit_checks(triggering_sha, repository)
    required_status = evaluate_required_checks(check_runs, required_checks)

    report = {
        "generated_at": dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "repository": repository,
        "triggering_sha": triggering_sha,
        "branch": args.branch,
        "page_url": page_url,
        "workflow_runs": workflow_runs,
        "required_check_status": required_status,
        "check_runs_count": len(check_runs or []),
        "check_runs": check_runs,
    }

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "devsecops-evidence.json"
    md_path = out_dir / "devsecops-evidence.md"

    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    lines = [
        "# DevSecOps Evidence Snapshot",
        "",
        f"- Repository: `{repository or 'unknown'}`",
        f"- Branch: `{args.branch}`",
        f"- Triggering SHA: `{triggering_sha or 'N/A'}`",
        f"- Generated: `{report['generated_at']}`",
    ]
    if page_url:
        lines.append(f"- Published URL: `{page_url}`")

    lines.append("")
    lines.append("## Required checks")
    for name, status in required_status.items():
        lines.append(f"- `{name}`: `{status}`")

    lines.append("")
    lines.append("## Workflow runs (latest)")
    for name, payload in workflow_runs.items():
        if not payload:
            lines.append(f"- `{name}`: `missing`")
            continue
        lines.append(
            "- `{name}`: status `{status}` / conclusion `{conclusion}` / url `{url}`".format(
                name=name,
                status=payload.get("status"),
                conclusion=payload.get("conclusion"),
                url=payload.get("url"),
            )
        )

    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"Wrote evidence report to {json_path}")
    print(f"Wrote evidence summary to {md_path}")


if __name__ == "__main__":
    main()
