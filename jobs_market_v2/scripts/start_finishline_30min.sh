#!/bin/zsh
set -euo pipefail

ROOT="/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2"
AUTOMATION_DIR="$ROOT/runtime/automation"
PID_FILE="$AUTOMATION_DIR/finishline_30min_loop.pid"
LAUNCHER_PID_FILE="$AUTOMATION_DIR/finishline_30min_launcher.pid"
LOOP_LOG="$AUTOMATION_DIR/finishline_30min_loop.out"
LOCK_DIR="$AUTOMATION_DIR/finishline_30min.lock"

mkdir -p "$AUTOMATION_DIR"

if [[ -f "$PID_FILE" ]]; then
  EXISTING_PID="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "$EXISTING_PID" ]] && kill -0 "$EXISTING_PID" 2>/dev/null; then
    echo "already running: $EXISTING_PID"
    exit 0
  fi
fi

if [[ -f "$LAUNCHER_PID_FILE" ]]; then
  EXISTING_LAUNCHER_PID="$(cat "$LAUNCHER_PID_FILE" 2>/dev/null || true)"
  if [[ -n "$EXISTING_LAUNCHER_PID" ]] && kill -0 "$EXISTING_LAUNCHER_PID" 2>/dev/null; then
    echo "launcher already queued: $EXISTING_LAUNCHER_PID"
    exit 0
  fi
fi

if [[ ! -d "$LOCK_DIR" ]]; then
  nohup /bin/zsh "$ROOT/scripts/run_finishline_30min_loop.sh" >> "$LOOP_LOG" 2>&1 &
  echo $! > "$PID_FILE"
  echo "finishline loop started: $!"
  exit 0
fi

nohup /bin/zsh -lc "
  while [[ -d '$LOCK_DIR' ]]; do
    sleep 15
  done
  exec /bin/zsh '$ROOT/scripts/run_finishline_30min_loop.sh'
" >> "$LOOP_LOG" 2>&1 &

echo $! > "$LAUNCHER_PID_FILE"
echo "finishline launcher queued"
