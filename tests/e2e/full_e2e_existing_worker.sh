#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
RUNTIME_DIR="${RUNNER_TEMP:-/tmp}"

: "${CLOUDFLARE_API_TOKEN:?CLOUDFLARE_API_TOKEN is required}"
: "${API_KEY:?API_KEY is required for /jobs endpoint}"
: "${WORKER_URL:?WORKER_URL is required}"

RUN_ID="${GITHUB_RUN_ID:-local}"
ATTEMPT="${GITHUB_RUN_ATTEMPT:-0}"
JOB_MARKER="gha-endpoint-${RUN_ID}-${ATTEMPT}"

RESP="$(curl -fsS -X POST "${WORKER_URL%/}/jobs" \
  -H 'content-type: application/json' \
  -H "x-api-key: $API_KEY" \
  -d "{\"input\":{\"exec_mode\":\"host\",\"command\":\"echo endpoint-ok-${JOB_MARKER}\",\"source\":\"github-actions-endpoint\",\"marker\":\"$JOB_MARKER\"}}")"

echo "$RESP" | grep -q '"status":"queued"'
JOB_ID="$(echo "$RESP" | python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["job_id"])')"

PYTHONUNBUFFERED=1 \
CF_QUEUES_API_TOKEN="$CLOUDFLARE_API_TOKEN" \
python3 "$ROOT_DIR/hpc-consumer/hpc_pull_consumer.py" > "$RUNTIME_DIR/hpc.log" 2>&1 &
HPC_PID=$!
sleep 8
kill "$HPC_PID" >/dev/null 2>&1 || true
cat "$RUNTIME_DIR/hpc.log"

PYTHONUNBUFFERED=1 \
CF_QUEUES_API_TOKEN="$CLOUDFLARE_API_TOKEN" \
python3 "$ROOT_DIR/local-consumer/local_pull_results.py" > "$RUNTIME_DIR/local.log" 2>&1
cat "$RUNTIME_DIR/local.log"

grep -q "$JOB_ID" "$RUNTIME_DIR/local.log"
test -f "$ROOT_DIR/results/$JOB_ID/output.json"

echo "full existing-worker e2e ok for job $JOB_ID"
