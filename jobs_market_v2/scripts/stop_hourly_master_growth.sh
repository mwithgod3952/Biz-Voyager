#!/bin/zsh
set -euo pipefail

ROOT="/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2"
PID_FILE="$ROOT/runtime/automation/hourly_master_growth_loop.pid"
LAUNCHER_PID_FILE="$ROOT/runtime/automation/hourly_master_growth_launcher.pid"

if [[ ! -f "$PID_FILE" ]]; then
  :
fi

if [[ -f "$PID_FILE" ]]; then
  PID="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then
    kill "$PID"
    echo "stopped loop: $PID"
  else
    echo "stale loop pid file"
  fi
fi

rm -f "$PID_FILE"

if [[ -f "$LAUNCHER_PID_FILE" ]]; then
  LAUNCHER_PID="$(cat "$LAUNCHER_PID_FILE" 2>/dev/null || true)"
  if [[ -n "$LAUNCHER_PID" ]] && kill -0 "$LAUNCHER_PID" 2>/dev/null; then
    kill "$LAUNCHER_PID"
    echo "stopped launcher: $LAUNCHER_PID"
  else
    echo "stale launcher pid file"
  fi
fi

rm -f "$LAUNCHER_PID_FILE"
