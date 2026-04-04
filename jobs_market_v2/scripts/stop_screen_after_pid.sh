#!/bin/zsh
set -euo pipefail

TARGET_PID="${1:?target pid required}"
TARGET_SESSION="${2:?target session required}"
LOG_FILE="${3:-/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/runtime/automation/finishline_stop_after_current.log}"

while kill -0 "$TARGET_PID" 2>/dev/null; do
  sleep 5
done

screen -S "$TARGET_SESSION" -X quit >/dev/null 2>&1 || true
echo "$(date '+%Y-%m-%d %H:%M:%S %Z') stopped screen session=$TARGET_SESSION after pid=$TARGET_PID" >> "$LOG_FILE"
