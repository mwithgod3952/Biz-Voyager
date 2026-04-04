#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "$SCRIPT_DIR/../.." rev-parse --show-toplevel)"
STATE_BRANCH="${1:-automation-state}"
WORKTREE_DIR="$(mktemp -d)"

cleanup() {
  git -C "$REPO_ROOT" worktree remove "$WORKTREE_DIR" --force >/dev/null 2>&1 || true
  rm -rf "$WORKTREE_DIR"
}
trap cleanup EXIT

if ! git -C "$REPO_ROOT" remote get-url origin >/dev/null 2>&1; then
  echo "origin remote is required before bootstrapping $STATE_BRANCH" >&2
  exit 1
fi

git -C "$REPO_ROOT" fetch origin >/dev/null 2>&1 || true

if git -C "$REPO_ROOT" ls-remote --exit-code origin "refs/heads/$STATE_BRANCH" >/dev/null 2>&1; then
  echo "$STATE_BRANCH already exists on origin"
  exit 0
fi

git -C "$REPO_ROOT" worktree add -b "$STATE_BRANCH" "$WORKTREE_DIR" HEAD >/dev/null
find "$WORKTREE_DIR" -mindepth 1 -maxdepth 1 ! -name .git -exec rm -rf {} +
mkdir -p "$WORKTREE_DIR/jobs_market_v2/.ci_state"
cat > "$WORKTREE_DIR/jobs_market_v2/.ci_state/workflow_state.json" <<'EOF'
{"consecutive_failures": 0, "last_result": "bootstrap"}
EOF

git -C "$WORKTREE_DIR" add jobs_market_v2/.ci_state/workflow_state.json
git -C "$WORKTREE_DIR" commit -m "Initialize automation state branch" >/dev/null
git -C "$WORKTREE_DIR" push -u origin "$STATE_BRANCH"

echo "$STATE_BRANCH initialized and pushed"
