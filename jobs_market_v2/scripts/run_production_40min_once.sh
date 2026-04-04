#!/bin/zsh
set -euo pipefail

ROOT="/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2"
AUTOMATION_DIR="$ROOT/runtime/automation"
LOCK_DIR="$AUTOMATION_DIR/production_40min.lock"
LOCK_PID_FILE="$LOCK_DIR/pid"
RUNNER_LOG="$AUTOMATION_DIR/production_40min_runner.log"
STATUS_SCRIPT="$ROOT/scripts/write_automation_status.py"
RUN_SCRIPT="$ROOT/scripts/run_production_cycle.sh"
VENV_PYTHON="$ROOT/.venv/bin/python"

mkdir -p "$AUTOMATION_DIR"

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  EXISTING_PID="$(cat "$LOCK_PID_FILE" 2>/dev/null || true)"
  if [[ -n "$EXISTING_PID" ]] && kill -0 "$EXISTING_PID" 2>/dev/null; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] skipped: production 40min run already in progress ($EXISTING_PID)" >> "$RUNNER_LOG"
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

RUN_TS="$(date '+%Y%m%d%H%M%S')"
LOG_FILE="$AUTOMATION_DIR/production_40min_${RUN_TS}.log"
PHASE="production_40min"

if [[ -x "$VENV_PYTHON" ]]; then
  "$VENV_PYTHON" "$STATUS_SCRIPT" start --phase "$PHASE" >/dev/null 2>&1 || true
fi

RESULT="success"
RESUME_NEXT_STEP="Wait for the next 40-minute production cycle trigger"

{
  echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] production 40min run started"
  /bin/bash "$RUN_SCRIPT"
  echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] production 40min run finished"
} >> "$LOG_FILE" 2>&1 || {
  RESULT="failure"
  RESUME_NEXT_STEP="Inspect runtime/automation/$(basename "$LOG_FILE") before the next scheduled trigger"
  echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] production 40min run failed" >> "$LOG_FILE"
}

if [[ -x "$VENV_PYTHON" ]]; then
  "$VENV_PYTHON" "$STATUS_SCRIPT" end --phase "$PHASE" --result "$RESULT" --resume-next-step "$RESUME_NEXT_STEP" >/dev/null 2>&1 || true
fi
