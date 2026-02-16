#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

: "${CLOUDFLARE_API_TOKEN:?CLOUDFLARE_API_TOKEN is required}"
RUNTIME_DIR="${RUNNER_TEMP:-/tmp}"
RUN_ID="${GITHUB_RUN_ID:-local}"
ATTEMPT="${GITHUB_RUN_ATTEMPT:-0}"
export JOB_ID="ci-e2e-${RUN_ID}-${ATTEMPT}"

cd "$ROOT_DIR"
python tests/e2e/enqueue_message.py

PYTHONUNBUFFERED=1 \
CF_QUEUES_API_TOKEN="$CLOUDFLARE_API_TOKEN" \
python hpc-consumer/hpc_pull_consumer.py > "$RUNTIME_DIR/hpc.log" 2>&1 &
HPC_PID=$!
sleep 8
kill "$HPC_PID" >/dev/null 2>&1 || true
cat "$RUNTIME_DIR/hpc.log"

PYTHONUNBUFFERED=1 \
CF_QUEUES_API_TOKEN="$CLOUDFLARE_API_TOKEN" \
python local-consumer/local_pull_results.py > "$RUNTIME_DIR/local.log" 2>&1
cat "$RUNTIME_DIR/local.log"

grep -q "$JOB_ID" "$RUNTIME_DIR/local.log"
test -f "$ROOT_DIR/results/$JOB_ID/output.json"

echo "cloudflare e2e ok for job $JOB_ID"
