#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

: "${CLOUDFLARE_API_TOKEN:?CLOUDFLARE_API_TOKEN is required}"
: "${CLOUDFLARE_ACCOUNT_ID:?CLOUDFLARE_ACCOUNT_ID is required}"

RUN_ID="${GITHUB_RUN_ID:-local}"
ATTEMPT="${GITHUB_RUN_ATTEMPT:-0}"
RUNTIME_DIR="${RUNNER_TEMP:-/tmp}"

JOBS_QUEUE="ci-jobs-${RUN_ID}-${ATTEMPT}"
RESULTS_QUEUE="ci-results-${RUN_ID}-${ATTEMPT}"

cleanup() {
  set +e
  cd "$ROOT_DIR/producer-worker"
  npx wrangler queues delete "$JOBS_QUEUE" --force >/dev/null 2>&1 || true
  npx wrangler queues delete "$RESULTS_QUEUE" --force >/dev/null 2>&1 || true
}
trap cleanup EXIT

cd "$ROOT_DIR/producer-worker"
npx wrangler queues create "$JOBS_QUEUE"
npx wrangler queues create "$RESULTS_QUEUE"

for _ in {1..10}; do
  npx wrangler queues consumer http add "$JOBS_QUEUE" && break
  sleep 2
done

for _ in {1..10}; do
  npx wrangler queues consumer http add "$RESULTS_QUEUE" && break
  sleep 2
done

JOBS_QUEUE_ID="$(npx wrangler queues info "$JOBS_QUEUE" | awk -F': ' '/Queue ID:/ {print $2}')"
RESULTS_QUEUE_ID="$(npx wrangler queues info "$RESULTS_QUEUE" | awk -F': ' '/Queue ID:/ {print $2}')"

if [[ -z "$JOBS_QUEUE_ID" || -z "$RESULTS_QUEUE_ID" ]]; then
  echo "failed to resolve queue IDs"
  exit 1
fi

export JOBS_QUEUE_ID RESULTS_QUEUE_ID
export JOB_ID="ci-e2e-${RUN_ID}-${ATTEMPT}"

cd "$ROOT_DIR"
python tests/e2e/enqueue_message.py

PYTHONUNBUFFERED=1 \
CF_QUEUES_API_TOKEN="$CLOUDFLARE_API_TOKEN" \
CF_ACCOUNT_ID="$CLOUDFLARE_ACCOUNT_ID" \
CF_JOBS_QUEUE_ID="$JOBS_QUEUE_ID" \
CF_RESULTS_QUEUE_ID="$RESULTS_QUEUE_ID" \
APPTAINER_BIN=/bin/echo \
APPTAINER_IMAGE=ci-dummy-image \
RESULTS_DIR="$RUNTIME_DIR/results" \
POLL_INTERVAL_SECONDS=1 \
python hpc-consumer/hpc_pull_consumer.py > "$RUNTIME_DIR/hpc.log" 2>&1 &
HPC_PID=$!
sleep 8
kill "$HPC_PID" >/dev/null 2>&1 || true
cat "$RUNTIME_DIR/hpc.log"

PYTHONUNBUFFERED=1 \
CF_QUEUES_API_TOKEN="$CLOUDFLARE_API_TOKEN" \
CF_ACCOUNT_ID="$CLOUDFLARE_ACCOUNT_ID" \
CF_RESULTS_QUEUE_ID="$RESULTS_QUEUE_ID" \
python local-consumer/local_pull_results.py > "$RUNTIME_DIR/local.log" 2>&1
cat "$RUNTIME_DIR/local.log"

grep -q "$JOB_ID" "$RUNTIME_DIR/local.log"
test -f "$RUNTIME_DIR/results/$JOB_ID/output.json"

echo "cloudflare e2e ok for job $JOB_ID"
