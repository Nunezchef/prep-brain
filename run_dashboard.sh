#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
API_HOST="${API_HOST:-127.0.0.1}"
API_PORT="${API_PORT:-8000}"
VITE_PORT="${VITE_PORT:-5173}"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"

if [ ! -x "$PYTHON_BIN" ]; then
  PYTHON_BIN="python3"
fi

cleanup() {
  if [ -n "${API_PID:-}" ]; then
    kill "$API_PID" >/dev/null 2>&1 || true
  fi
}

trap cleanup EXIT INT TERM

if ! "$PYTHON_BIN" -c "import fastapi, uvicorn" >/dev/null 2>&1; then
  echo "FastAPI/uvicorn not installed in current Python. Run: pip install -r requirements.txt"
  exit 1
fi

if ! curl -fsS "http://$API_HOST:$API_PORT/api/health" >/dev/null 2>&1; then
  cd "$ROOT_DIR"
  "$PYTHON_BIN" -m uvicorn services.dashboard_api:app --host "$API_HOST" --port "$API_PORT" >/tmp/prep-brain-api.log 2>&1 &
  API_PID=$!
  sleep 1
fi

cd "$ROOT_DIR/frontend"
if [ ! -d node_modules ]; then
  npm install
fi

export VITE_API_BASE_URL="http://$API_HOST:$API_PORT"
npm run dev -- --host 0.0.0.0 --port "$VITE_PORT"
