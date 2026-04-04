#!/bin/zsh
set -euo pipefail

ROOT="/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2"
AUTOMATION_DIR="$ROOT/runtime/automation"
STATE_FILE="$AUTOMATION_DIR/finishline_campaign.state"
LOCK_DIR="$AUTOMATION_DIR/finishline_30min.lock"
PLIST_PATH="/Users/junheelee/Library/LaunchAgents/com.junheelee.jobs-market-v2.finishline.plist"

if [[ -f "$STATE_FILE" ]]; then
  cat > "$STATE_FILE" <<EOF
active=0
completed_iterations=0
total_iterations=0
sleep_seconds=60
next_eligible_epoch=0
last_tick_epoch=$(date +%s)
EOF
fi

rm -rf "$LOCK_DIR"
launchctl bootout "gui/$(id -u)" "$PLIST_PATH" >/dev/null 2>&1 || true
echo "finishline campaign stopped"
