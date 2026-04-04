#!/bin/zsh
set -euo pipefail

ROOT="/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2"
RUNTIME_DIR="$ROOT/runtime"
AUTOMATION_DIR="$RUNTIME_DIR/automation"
LOCK_DIR="$AUTOMATION_DIR/hourly_master_growth.lock"
PROMPT_FILE="$ROOT/scripts/hourly_master_growth_prompt.txt"
STATUS_SCRIPT="$ROOT/scripts/write_automation_status.py"
LAST_MESSAGE_FILE="$AUTOMATION_DIR/last_message.txt"

mkdir -p "$AUTOMATION_DIR"

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] skipped: hourly master growth run already in progress" >> "$AUTOMATION_DIR/runner.log"
  exit 0
fi

cleanup() {
  rmdir "$LOCK_DIR" >/dev/null 2>&1 || true
}
trap cleanup EXIT

cd "$ROOT"
source .venv/bin/activate

RUN_TS="$(date '+%Y%m%d%H%M%S')"
LOG_FILE="$AUTOMATION_DIR/hourly_master_growth_${RUN_TS}.log"

python "$STATUS_SCRIPT" start --phase kickoff

RESULT="success"
RESUME_NEXT_STEP="Read docs/HANDOFF.md and continue from the top growth bottleneck"

{
  echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] run started"
  codex exec \
    -C "$ROOT" \
    -m gpt-5.4 \
    --dangerously-bypass-approvals-and-sandbox \
    --output-last-message "$LAST_MESSAGE_FILE" \
    "$(cat "$PROMPT_FILE")"
  echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] run finished"
} >> "$LOG_FILE" 2>&1 || {
  RESULT="failure"
  RESUME_NEXT_STEP="Inspect runtime/automation/$(basename "$LOG_FILE"), repair the failure, then continue from docs/HANDOFF.md"
  echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] run failed" >> "$LOG_FILE"
}

python "$STATUS_SCRIPT" end --phase finished --result "$RESULT" --resume-next-step "$RESUME_NEXT_STEP"
