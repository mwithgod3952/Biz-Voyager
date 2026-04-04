#!/bin/zsh
set -euo pipefail

SESSION_NAME="finishline_campaign"

if screen -list | grep -Fq ".$SESSION_NAME"; then
  screen -S "$SESSION_NAME" -X quit
  echo "finishline screen stopped"
else
  echo "finishline screen not running"
fi
