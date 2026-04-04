#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ ! -d ".venv" ]]; then
  echo ".venv not found. Run ./scripts/setup_env.sh first." >&2
  exit 1
fi

source .venv/bin/activate

python -m jobs_market_v2.cli doctor
python -m jobs_market_v2.cli run-collection-cycle
