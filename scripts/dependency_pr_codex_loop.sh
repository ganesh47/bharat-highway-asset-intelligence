#!/usr/bin/env bash

set -euo pipefail

REPO="${REPO:-ganesh47/bharat-highway-asset-intelligence}"
WORKDIR_ROOT="${WORKDIR_ROOT:-$HOME/.openclaw/workspace/automation}"
REPO_DIR="${REPO_DIR:-$WORKDIR_ROOT/$(basename "$REPO")}"
STATE_DIR="${STATE_DIR:-$WORKDIR_ROOT/state}"
LOG_DIR="${LOG_DIR:-$WORKDIR_ROOT/logs}"
TARGET_PR="${TARGET_PR:-}"
DEFAULT_BASE="${DEFAULT_BASE:-main}"

mkdir -p "$WORKDIR_ROOT" "$STATE_DIR" "$LOG_DIR"

timestamp() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

log() {
  printf '[%s] %s\n' "$(timestamp)" "$*"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

require_cmd gh
require_cmd git
require_cmd jq
require_cmd python3

if ! command -v codex >/dev/null 2>&1; then
  echo "Missing required command: codex" >&2
  echo "Install Codex in the grclaw sandbox host before scheduling this loop." >&2
  exit 1
fi

if [ ! -d "$REPO_DIR/.git" ]; then
  log "Cloning $REPO into $REPO_DIR"
  git clone "https://github.com/$REPO.git" "$REPO_DIR"
fi

cd "$REPO_DIR"
git remote set-url origin "https://github.com/$REPO.git"
git fetch origin --prune

pick_pr() {
  if [ -n "$TARGET_PR" ]; then
    printf '%s' "$TARGET_PR"
    return 0
  fi

  gh pr list \
    --repo "$REPO" \
    --state open \
    --json number,author,labels,isDraft,statusCheckRollup,headRefName \
    --jq '
      map(
        select(.isDraft == false)
        | select(.author.login == "dependabot[bot]" or .author.login == "app/dependabot")
        | select(any(.labels[]?; .name == "dependencies"))
        | select(any(.statusCheckRollup[]?; .conclusion == "FAILURE" or .conclusion == "TIMED_OUT" or .conclusion == "ACTION_REQUIRED"))
      )
      | first
      | .number // ""
    '
}

PR_NUMBER="$(pick_pr)"

if [ -z "$PR_NUMBER" ]; then
  log "No failing dependency PRs found, exiting"
  exit 0
fi

PR_JSON="$(gh pr view "$PR_NUMBER" --repo "$REPO" --json number,title,body,headRefName,baseRefName,url,statusCheckRollup,commits,files)"
PR_BRANCH="$(printf '%s' "$PR_JSON" | jq -r '.headRefName')"
BASE_BRANCH="$(printf '%s' "$PR_JSON" | jq -r '.baseRefName // "'"$DEFAULT_BASE"'"')"
PR_URL="$(printf '%s' "$PR_JSON" | jq -r '.url')"

log "Selected PR #$PR_NUMBER ($PR_URL) on branch $PR_BRANCH"

git checkout "$BASE_BRANCH"
git pull --ff-only origin "$BASE_BRANCH"

if git show-ref --quiet "refs/heads/$PR_BRANCH"; then
  git checkout "$PR_BRANCH"
  git reset --hard "origin/$PR_BRANCH"
else
  git checkout -b "$PR_BRANCH" "origin/$PR_BRANCH"
fi

CONTEXT_JSON="$STATE_DIR/pr-${PR_NUMBER}-context.json"
PROMPT_TXT="$STATE_DIR/pr-${PR_NUMBER}-codex-prompt.txt"
RUN_LOG="$LOG_DIR/pr-${PR_NUMBER}-$(date -u +%Y%m%dT%H%M%SZ).log"

printf '%s\n' "$PR_JSON" > "$CONTEXT_JSON"

cat > "$PROMPT_TXT" <<EOF
Repository: $REPO
Pull Request: #$PR_NUMBER
URL: $PR_URL

Goal:
- Fix this failing dependency PR with the smallest safe change set.

Rules:
- Only make changes directly related to the dependency update or broken CI caused by it.
- Do not weaken security checks or branch protections.
- Do not refactor unrelated code.
- Keep GitHub Pages deployment safe.
- After edits, run the local validation commands and stop if they fail.

Required local validation commands:
1. python3 -m compileall -q .
2. python3 scripts/validate_artifacts.py --inventory research/source_inventory.yaml --catalog data/manifests/catalog.json --manifests data/manifests

PR context JSON path:
$CONTEXT_JSON
EOF

log "Running Codex remediation for PR #$PR_NUMBER"
codex exec --cwd "$REPO_DIR" --input "$PROMPT_TXT" 2>&1 | tee "$RUN_LOG"

log "Running validation after Codex"
python3 -m compileall -q .
python3 scripts/validate_artifacts.py --inventory research/source_inventory.yaml --catalog data/manifests/catalog.json --manifests data/manifests

if git diff --quiet; then
  log "No changes produced by Codex, exiting"
  exit 0
fi

git add -A
git commit -m "fix(deps): codex remediation for PR #$PR_NUMBER"
git push origin HEAD:"$PR_BRANCH"

gh pr comment "$PR_NUMBER" --repo "$REPO" --body "grclaw local Codex remediation ran in sandbox, pushed a minimal fix attempt, and reran local validation. Please review the diff and fresh CI results."

log "Done"
