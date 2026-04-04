#!/bin/zsh
set -euo pipefail

ROOT="/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2"
AUTOMATION_DIR="$ROOT/runtime/automation"
PID_FILE="$AUTOMATION_DIR/production_40min_loop.pid"
LOOP_LOG="$AUTOMATION_DIR/production_40min_loop.out"
SLEEP_SECONDS="${PRODUCTION_40MIN_SLEEP_SECONDS:-2400}"

mkdir -p "$AUTOMATION_DIR"
echo "$$" > "$PID_FILE"

cleanup() {
  rm -f "$PID_FILE"
}
trap cleanup EXIT

while true; do
  /bin/zsh "$ROOT/scripts/run_production_40min_once.sh" >> "$LOOP_LOG" 2>&1
  sleep "$SLEEP_SECONDS"
done
