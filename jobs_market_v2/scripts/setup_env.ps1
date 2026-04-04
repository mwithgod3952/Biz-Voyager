$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

python -m venv .venv
& ".venv\Scripts\python.exe" -m pip install --upgrade pip
& ".venv\Scripts\python.exe" -m pip install -e .
