#!/bin/zsh
set -euo pipefail

ROOT="/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2"
PID_FILE="$ROOT/runtime/automation/finishline_30min_loop.pid"
LAUNCHER_PID_FILE="$ROOT/runtime/automation/finishline_30min_launcher.pid"
LOCK_DIR="$ROOT/runtime/automation/finishline_30min.lock"

if [[ -f "$PID_FILE" ]]; then
  PID="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then
    kill "$PID"
    echo "stopped finishline loop: $PID"
  else
    echo "stale finishline loop pid file"
  fi
  rm -f "$PID_FILE"
else
  echo "finishline loop not running"
fi

if [[ -f "$LAUNCHER_PID_FILE" ]]; then
  LAUNCHER_PID="$(cat "$LAUNCHER_PID_FILE" 2>/dev/null || true)"
  if [[ -n "$LAUNCHER_PID" ]] && kill -0 "$LAUNCHER_PID" 2>/dev/null; then
    kill "$LAUNCHER_PID"
    echo "stopped finishline launcher: $LAUNCHER_PID"
  else
    echo "stale finishline launcher pid file"
  fi
  rm -f "$LAUNCHER_PID_FILE"
fi

rm -rf "$LOCK_DIR"
