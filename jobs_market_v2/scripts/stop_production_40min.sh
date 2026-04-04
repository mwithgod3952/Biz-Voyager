#!/bin/zsh
set -euo pipefail

ROOT="/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2"
AUTOMATION_DIR="$ROOT/runtime/automation"
LOCK_DIR="$AUTOMATION_DIR/production_40min.lock"
LOCK_PID_FILE="$LOCK_DIR/pid"
PID_FILE="$AUTOMATION_DIR/production_40min_loop.pid"
PLIST_PATH="/Users/junheelee/Library/LaunchAgents/com.junheelee.jobs-market-v2.production-40min.plist"
SESSION_NAME="jobs_market_v2_production_40min"
STOP_LOG="$AUTOMATION_DIR/production_40min_stop_after_current.log"

launchctl bootout "gui/$(id -u)" "$PLIST_PATH" >/dev/null 2>&1 || true

if [[ -f "$LOCK_PID_FILE" ]]; then
  RUN_PID="$(cat "$LOCK_PID_FILE" 2>/dev/null || true)"
  if [[ -n "$RUN_PID" ]] && kill -0 "$RUN_PID" 2>/dev/null; then
    nohup /bin/zsh "$ROOT/scripts/stop_screen_after_pid.sh" "$RUN_PID" "$SESSION_NAME" "$STOP_LOG" >/dev/null 2>&1 &
    echo "current run still finishing: $RUN_PID"
    echo "screen session will stop after current run: $SESSION_NAME"
    exit 0
  fi
fi

SCREEN_LIST="$(screen -ls 2>/dev/null || true)"
if [[ "$SCREEN_LIST" == *"$SESSION_NAME"* ]]; then
  screen -S "$SESSION_NAME" -X quit >/dev/null 2>&1 || true
  echo "stopped production 40min screen session: $SESSION_NAME"
fi

rm -f "$PID_FILE"
rm -rf "$LOCK_DIR"
echo "production 40min automation stopped"
