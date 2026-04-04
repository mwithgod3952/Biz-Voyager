#!/bin/zsh
set -euo pipefail

ROOT="/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2"
AUTOMATION_DIR="$ROOT/runtime/automation"
LOCK_PID_FILE="$AUTOMATION_DIR/production_40min.lock/pid"
LOOP_PID_FILE="$AUTOMATION_DIR/production_40min_loop.pid"
PLIST_PATH="/Users/junheelee/Library/LaunchAgents/com.junheelee.jobs-market-v2.production-40min.plist"
LABEL="com.junheelee.jobs-market-v2.production-40min"
STATUS_JSON="$ROOT/runtime/automation_status.json"
SESSION_NAME="jobs_market_v2_production_40min"

echo "label: $LABEL"
echo "plist: $PLIST_PATH"

if launchctl print "gui/$(id -u)/$LABEL" >/dev/null 2>&1; then
  echo "launchd_loaded: yes"
else
  echo "launchd_loaded: no"
fi

SCREEN_LIST="$(screen -ls 2>/dev/null || true)"
if [[ "$SCREEN_LIST" == *"$SESSION_NAME"* ]]; then
  echo "screen_session: running ($SESSION_NAME)"
else
  echo "screen_session: none"
fi

if [[ -f "$LOOP_PID_FILE" ]]; then
  LOOP_PID="$(cat "$LOOP_PID_FILE" 2>/dev/null || true)"
  if [[ -n "$LOOP_PID" ]] && kill -0 "$LOOP_PID" 2>/dev/null; then
    echo "loop_pid: $LOOP_PID"
  else
    echo "loop_pid: stale"
  fi
else
  echo "loop_pid: none"
fi

if [[ -f "$LOCK_PID_FILE" ]]; then
  RUN_PID="$(cat "$LOCK_PID_FILE" 2>/dev/null || true)"
  if [[ -n "$RUN_PID" ]] && kill -0 "$RUN_PID" 2>/dev/null; then
    echo "current_run_pid: $RUN_PID"
  else
    echo "current_run_pid: stale"
  fi
else
  echo "current_run_pid: none"
fi

if [[ -f "$STATUS_JSON" ]]; then
  echo "automation_status_json: $STATUS_JSON"
  /bin/cat "$STATUS_JSON"
else
  echo "automation_status_json: missing"
fi
