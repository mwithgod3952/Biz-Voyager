#!/bin/zsh
set -euo pipefail

ROOT="/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2"
AUTOMATION_DIR="$ROOT/runtime/automation"
PID_FILE="$AUTOMATION_DIR/finishline_30min_loop.pid"
LOOP_LOG="$AUTOMATION_DIR/finishline_30min_loop.out"
ITERATIONS="${FINISHLINE_ITERATIONS:-4}"
SLEEP_SECONDS="${FINISHLINE_SLEEP_SECONDS:-60}"

mkdir -p "$AUTOMATION_DIR"
echo "$$" > "$PID_FILE"

cleanup() {
  rm -f "$PID_FILE"
}
trap cleanup EXIT

for ((iteration=1; iteration<=ITERATIONS; iteration++)); do
  /bin/zsh "$ROOT/scripts/run_finishline_30min_once.sh" "$iteration" "$ITERATIONS" >> "$LOOP_LOG" 2>&1
  if (( iteration < ITERATIONS )); then
    sleep "$SLEEP_SECONDS"
  fi
done
