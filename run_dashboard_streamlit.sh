#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"
source .venv/bin/activate
export PYTHONPATH="$PYTHONPATH:$(pwd)"
streamlit run dashboard/app.py
