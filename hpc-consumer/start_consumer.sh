#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$REPO_DIR/.env"
PID_FILE="$REPO_DIR/hpc-consumer/hpc_supervisor.pid"
LOG_FILE="$REPO_DIR/hpc-consumer/hpc_pull_consumer.log"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing $ENV_FILE"
  exit 1
fi

if [[ -f "$PID_FILE" ]]; then
  PID="$(cat "$PID_FILE" || true)"
  if [[ -n "$PID" ]] && kill -0 "$PID" >/dev/null 2>&1; then
    echo "consumer already running with pid $PID"
    exit 0
  fi
fi

set -a
source "$ENV_FILE"
set +a

mkdir -p "$REPO_DIR/hpc-consumer"
PYTHON_BIN="${PYTHON_BIN:-python3}"
nohup "$PYTHON_BIN" "$REPO_DIR/hpc-consumer/hpc_supervisor.py" >> "$LOG_FILE" 2>&1 &
NEW_PID=$!
echo "$NEW_PID" > "$PID_FILE"

echo "started supervisor pid=$NEW_PID"
