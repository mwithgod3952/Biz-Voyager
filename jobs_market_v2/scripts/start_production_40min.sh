#!/bin/zsh
set -euo pipefail

ROOT="/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2"
AUTOMATION_DIR="$ROOT/runtime/automation"
PID_FILE="$AUTOMATION_DIR/production_40min_loop.pid"
LOOP_LOG="$AUTOMATION_DIR/production_40min_loop.out"
PLIST_PATH="/Users/junheelee/Library/LaunchAgents/com.junheelee.jobs-market-v2.production-40min.plist"
SESSION_NAME="jobs_market_v2_production_40min"

mkdir -p "$AUTOMATION_DIR"
launchctl bootout "gui/$(id -u)" "$PLIST_PATH" >/dev/null 2>&1 || true

SCREEN_LIST="$(screen -ls 2>/dev/null || true)"
if [[ "$SCREEN_LIST" == *"$SESSION_NAME"* ]]; then
  echo "already running in screen: $SESSION_NAME"
  exit 0
fi

if [[ -f "$PID_FILE" ]]; then
  EXISTING_PID="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "$EXISTING_PID" ]] && kill -0 "$EXISTING_PID" 2>/dev/null; then
    echo "already running: $EXISTING_PID"
    exit 0
  fi
fi

screen -dmS "$SESSION_NAME" /bin/zsh "$ROOT/scripts/run_production_40min_loop.sh"
sleep 1

SCREEN_LIST="$(screen -ls 2>/dev/null || true)"
if [[ "$SCREEN_LIST" == *"$SESSION_NAME"* ]]; then
  echo "production 40min loop started in screen: $SESSION_NAME"
  exit 0
fi

echo "failed to start production 40min screen session" >&2
exit 1
