#!/bin/zsh
set -euo pipefail

ROOT="/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2"
AUTOMATION_DIR="$ROOT/runtime/automation"
STATE_FILE="$AUTOMATION_DIR/finishline_campaign.state"
LOCK_DIR="$AUTOMATION_DIR/finishline_30min.lock"
PLIST_DIR="/Users/junheelee/Library/LaunchAgents"
PLIST_PATH="$PLIST_DIR/com.junheelee.jobs-market-v2.finishline.plist"
STDOUT_LOG="$AUTOMATION_DIR/finishline_launchd.out"
STDERR_LOG="$AUTOMATION_DIR/finishline_launchd.err"
TICK_SCRIPT="$ROOT/scripts/run_finishline_campaign_tick.sh"
LABEL="com.junheelee.jobs-market-v2.finishline"
TOTAL_ITERATIONS="${FINISHLINE_ITERATIONS:-4}"
SLEEP_SECONDS="${FINISHLINE_SLEEP_SECONDS:-60}"

mkdir -p "$AUTOMATION_DIR" "$PLIST_DIR"
rm -rf "$LOCK_DIR"

cat > "$STATE_FILE" <<EOF
active=1
completed_iterations=0
total_iterations=$TOTAL_ITERATIONS
sleep_seconds=$SLEEP_SECONDS
next_eligible_epoch=0
last_tick_epoch=$(date +%s)
EOF

cat > "$PLIST_PATH" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/zsh</string>
    <string>-lc</string>
    <string>/bin/zsh "$TICK_SCRIPT"</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>StartInterval</key>
  <integer>60</integer>
  <key>StandardOutPath</key>
  <string>$STDOUT_LOG</string>
  <key>StandardErrorPath</key>
  <string>$STDERR_LOG</string>
  <key>WorkingDirectory</key>
  <string>$ROOT</string>
</dict>
</plist>
EOF

launchctl bootout "gui/$(id -u)" "$PLIST_PATH" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "$PLIST_PATH"
launchctl kickstart -k "gui/$(id -u)/$LABEL"

echo "finishline campaign started via launchd"
