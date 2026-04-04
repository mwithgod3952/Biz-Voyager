#!/bin/zsh
set -euo pipefail

ROOT="/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2"
AUTOMATION_DIR="$ROOT/runtime/automation"
STATE_FILE="$AUTOMATION_DIR/finishline_campaign.state"
LOCK_DIR="$AUTOMATION_DIR/finishline_30min.lock"
LOCK_PID_FILE="$LOCK_DIR/pid"
ONCE_SCRIPT="$ROOT/scripts/run_finishline_30min_once.sh"
PLIST_PATH="/Users/junheelee/Library/LaunchAgents/com.junheelee.jobs-market-v2.finishline.plist"
LABEL="com.junheelee.jobs-market-v2.finishline"

mkdir -p "$AUTOMATION_DIR"

if [[ ! -f "$STATE_FILE" ]]; then
  exit 0
fi

source "$STATE_FILE"

active="${active:-0}"
completed_iterations="${completed_iterations:-0}"
total_iterations="${total_iterations:-4}"
sleep_seconds="${sleep_seconds:-60}"
next_eligible_epoch="${next_eligible_epoch:-0}"

save_state() {
  cat > "$STATE_FILE" <<EOF
active=$active
completed_iterations=$completed_iterations
total_iterations=$total_iterations
sleep_seconds=$sleep_seconds
next_eligible_epoch=$next_eligible_epoch
last_tick_epoch=$(date +%s)
EOF
}

stop_job_if_done() {
  if (( completed_iterations >= total_iterations )); then
    active=0
    save_state
    launchctl bootout "gui/$(id -u)" "$PLIST_PATH" >/dev/null 2>&1 || true
    exit 0
  fi
}

if [[ "$active" != "1" ]]; then
  stop_job_if_done
  exit 0
fi

stop_job_if_done

now_epoch="$(date +%s)"
if (( now_epoch < next_eligible_epoch )); then
  exit 0
fi

if [[ -d "$LOCK_DIR" ]]; then
  EXISTING_PID="$(cat "$LOCK_PID_FILE" 2>/dev/null || true)"
  if [[ -n "$EXISTING_PID" ]] && kill -0 "$EXISTING_PID" 2>/dev/null; then
    exit 0
  fi
  rm -rf "$LOCK_DIR"
fi

iteration=$((completed_iterations + 1))
/bin/zsh "$ONCE_SCRIPT" "$iteration" "$total_iterations"
completed_iterations="$iteration"
next_eligible_epoch=$(( $(date +%s) + sleep_seconds ))
save_state
stop_job_if_done
