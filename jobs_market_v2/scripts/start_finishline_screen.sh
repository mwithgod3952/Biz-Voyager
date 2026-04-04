#!/bin/zsh
set -euo pipefail

ROOT="/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2"
SESSION_NAME="finishline_campaign"

if screen -list | grep -Fq ".$SESSION_NAME"; then
  echo "finishline screen already running"
  exit 0
fi

screen -dmS "$SESSION_NAME" /bin/zsh -lc "
  export FINISHLINE_ITERATIONS=4
  export FINISHLINE_SLEEP_SECONDS=60
  exec /bin/zsh '$ROOT/scripts/run_finishline_30min_loop.sh'
"

echo "finishline screen started"
