#!/bin/zsh
set -euo pipefail

ROOT="/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2"
RUNTIME_DIR="$ROOT/runtime"
AUTOMATION_DIR="$RUNTIME_DIR/automation"
LOCK_DIR="$AUTOMATION_DIR/finishline_30min.lock"
LOCK_PID_FILE="$LOCK_DIR/pid"
PROMPT_FILE="$ROOT/scripts/finishline_30min_prompt.txt"
STATUS_SCRIPT="$ROOT/scripts/write_automation_status.py"
LAST_MESSAGE_FILE="$AUTOMATION_DIR/finishline_30min_last_message.txt"
CODEX_BIN="/Applications/Codex.app/Contents/Resources/codex"

ITERATION="${1:-1}"
TOTAL="${2:-4}"

mkdir -p "$AUTOMATION_DIR"

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  EXISTING_PID="$(cat "$LOCK_PID_FILE" 2>/dev/null || true)"
  if [[ -n "$EXISTING_PID" ]] && kill -0 "$EXISTING_PID" 2>/dev/null; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] skipped: finishline 30min run already in progress ($EXISTING_PID)" >> "$AUTOMATION_DIR/finishline_30min_runner.log"
    exit 0
  fi
  rm -rf "$LOCK_DIR"
  mkdir "$LOCK_DIR"
fi

echo "$$" > "$LOCK_PID_FILE"

cleanup() {
  rm -f "$LOCK_PID_FILE" >/dev/null 2>&1 || true
  rmdir "$LOCK_DIR" >/dev/null 2>&1 || true
}
trap cleanup EXIT

cd "$ROOT"
source .venv/bin/activate

RUN_TS="$(date '+%Y%m%d%H%M%S')"
LOG_FILE="$AUTOMATION_DIR/finishline_30min_${RUN_TS}.log"
PHASE="iteration_${ITERATION}_of_${TOTAL}"

python "$STATUS_SCRIPT" start --phase "$PHASE"

RESULT="success"
RESUME_NEXT_STEP="Read docs/HANDOFF.md and continue from the top growth bottleneck"

{
  echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] run started ($PHASE)"
  "$CODEX_BIN" exec \
    -C "$ROOT" \
    -m gpt-5.4 \
    --dangerously-bypass-approvals-and-sandbox \
    --output-last-message "$LAST_MESSAGE_FILE" \
    "$(cat "$PROMPT_FILE")"
  echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] run finished ($PHASE)"
} >> "$LOG_FILE" 2>&1 || {
  RESULT="failure"
  RESUME_NEXT_STEP="Inspect runtime/automation/$(basename "$LOG_FILE"), repair the failure, then continue from docs/HANDOFF.md"
  echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] run failed ($PHASE)" >> "$LOG_FILE"
}

python "$STATUS_SCRIPT" end --phase "$PHASE" --result "$RESULT" --resume-next-step "$RESUME_NEXT_STEP"
