#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
RUNTIME_DIR="${RUNNER_TEMP:-/tmp}"

: "${CLOUDFLARE_API_TOKEN:?CLOUDFLARE_API_TOKEN is required}"
: "${API_KEY:?API_KEY is required for /jobs endpoint}"
: "${WORKER_URL:?WORKER_URL is required}"
: "${CLOUDFLARE_ACCOUNT_ID:=59908b351c3a3321ff84dd2d78bf0b42}"
: "${CF_JOBS_QUEUE_ID:=f52e2e6bb569425894ede9141e9343a5}"
: "${CF_RESULTS_QUEUE_ID:=a435ae20f7514ce4b193879704b03e4e}"

RUN_ID="${GITHUB_RUN_ID:-local}"
ATTEMPT="${GITHUB_RUN_ATTEMPT:-0}"
JOB_MARKER="gha-endpoint-${RUN_ID}-${ATTEMPT}"

RESP="$(curl -fsS -X POST "${WORKER_URL%/}/jobs" \
  -H 'content-type: application/json' \
  -H "x-api-key: $API_KEY" \
  -d "{\"input\":{\"iterations\":3,\"source\":\"github-actions-endpoint\",\"marker\":\"$JOB_MARKER\"}}")"

echo "$RESP" | grep -q '"status":"queued"'
JOB_ID="$(echo "$RESP" | python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["job_id"])')"

PYTHONUNBUFFERED=1 \
CF_QUEUES_API_TOKEN="$CLOUDFLARE_API_TOKEN" \
CF_ACCOUNT_ID="$CLOUDFLARE_ACCOUNT_ID" \
CF_JOBS_QUEUE_ID="$CF_JOBS_QUEUE_ID" \
CF_RESULTS_QUEUE_ID="$CF_RESULTS_QUEUE_ID" \
APPTAINER_BIN=/bin/echo \
APPTAINER_IMAGE=ci-dummy-image \
RESULTS_DIR="$RUNTIME_DIR/results" \
POLL_INTERVAL_SECONDS=1 \
python3 "$ROOT_DIR/hpc-consumer/hpc_pull_consumer.py" > "$RUNTIME_DIR/hpc.log" 2>&1 &
HPC_PID=$!
sleep 8
kill "$HPC_PID" >/dev/null 2>&1 || true
cat "$RUNTIME_DIR/hpc.log"

PYTHONUNBUFFERED=1 \
CF_QUEUES_API_TOKEN="$CLOUDFLARE_API_TOKEN" \
CF_ACCOUNT_ID="$CLOUDFLARE_ACCOUNT_ID" \
CF_RESULTS_QUEUE_ID="$CF_RESULTS_QUEUE_ID" \
python3 "$ROOT_DIR/laptop-consumer/laptop_pull_results.py" > "$RUNTIME_DIR/laptop.log" 2>&1
cat "$RUNTIME_DIR/laptop.log"

grep -q "$JOB_ID" "$RUNTIME_DIR/laptop.log"
test -f "$RUNTIME_DIR/results/$JOB_ID/output.json"

echo "full existing-worker e2e ok for job $JOB_ID"
