#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
source .venv/bin/activate
python -m ipykernel install --user --name jobs-market-v2 --display-name "jobs-market-v2"
