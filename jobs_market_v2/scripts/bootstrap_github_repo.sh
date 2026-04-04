#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <github_repo_url>" >&2
  exit 1
fi

REMOTE_URL="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "$SCRIPT_DIR/../.." rev-parse --show-toplevel)"
BRANCH="$(git -C "$REPO_ROOT" branch --show-current || true)"

if [[ -z "$BRANCH" ]]; then
  BRANCH="master"
  git -C "$REPO_ROOT" checkout -B "$BRANCH"
fi

EXISTING_REMOTE="$(git -C "$REPO_ROOT" remote get-url origin 2>/dev/null || true)"
if [[ -n "$EXISTING_REMOTE" && "$EXISTING_REMOTE" != "$REMOTE_URL" ]]; then
  echo "origin already exists: $EXISTING_REMOTE" >&2
  exit 1
fi

if [[ -z "$EXISTING_REMOTE" ]]; then
  git -C "$REPO_ROOT" remote add origin "$REMOTE_URL"
fi

git -C "$REPO_ROOT" add -- .github .gitignore AGENTS.md jobs_market_v2

if ! git -C "$REPO_ROOT" diff --cached --quiet || ! git -C "$REPO_ROOT" rev-parse --verify HEAD >/dev/null 2>&1; then
  git -C "$REPO_ROOT" commit -m "Bootstrap jobs_market_v2 deployment"
fi

git -C "$REPO_ROOT" push -u origin "$BRANCH"

echo "repository bootstrap pushed to $REMOTE_URL on branch $BRANCH"
