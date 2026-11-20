#!/bin/bash
# 本地启动 FastAPI（对接 local_pg.sh 的两个实例），状态库放在 .localrun。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
mkdir -p "$ROOT/.localrun/data"

export OLD_DSN="host=127.0.0.1 port=55433 user=node dbname=inspection"
export NEW_DSN="host=127.0.0.1 port=55434 user=node dbname=inspection"
export STATE_DB_PATH="$ROOT/.localrun/data/handover_state.db"
export CUTOVER_TIMEOUT_SECONDS="${CUTOVER_TIMEOUT_SECONDS:-8}"
export SUBSCRIPTION_NAME="inspection_sub"

cd "$ROOT/app"
exec /workspace/.local/venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 8000
