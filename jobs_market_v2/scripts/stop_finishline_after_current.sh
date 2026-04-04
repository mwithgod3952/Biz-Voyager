#!/bin/zsh
set -euo pipefail

TARGET_PID="${1:?target pid required}"
SESSION_NAME="${2:-finishline_campaign}"
LOG_FILE="/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/runtime/automation/finishline_stop_after_current.log"

while kill -0 "$TARGET_PID" 2>/dev/null; do
  sleep 5
done

screen -S "$SESSION_NAME" -X quit >/dev/null 2>&1 || true
echo "$(date '+%Y-%m-%d %H:%M:%S %Z') stopped $SESSION_NAME after current iteration pid=$TARGET_PID" >> "$LOG_FILE"
