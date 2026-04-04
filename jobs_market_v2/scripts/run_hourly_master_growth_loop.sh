#!/bin/zsh
set -euo pipefail

ROOT="/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2"
AUTOMATION_DIR="$ROOT/runtime/automation"
PID_FILE="$AUTOMATION_DIR/hourly_master_growth_loop.pid"
LOOP_LOG="$AUTOMATION_DIR/hourly_master_growth_loop.out"

mkdir -p "$AUTOMATION_DIR"
echo "$$" > "$PID_FILE"

while true; do
  "$ROOT/scripts/run_hourly_master_growth_once.sh" >> "$LOOP_LOG" 2>&1
  sleep 3600
done
