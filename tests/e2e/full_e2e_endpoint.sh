#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
RUNTIME_DIR="${RUNNER_TEMP:-/tmp}"

: "${CLOUDFLARE_API_TOKEN:?CLOUDFLARE_API_TOKEN is required}"
: "${CLOUDFLARE_ACCOUNT_ID:?CLOUDFLARE_ACCOUNT_ID is required}"

RUN_ID="${GITHUB_RUN_ID:-local}"
ATTEMPT="${GITHUB_RUN_ATTEMPT:-0}"

WORKER_NAME="ci-e2e-worker-${RUN_ID}-${ATTEMPT}"
JOBS_QUEUE="ci-jobs-${RUN_ID}-${ATTEMPT}"
RESULTS_QUEUE="ci-results-${RUN_ID}-${ATTEMPT}"
API_KEY="ci-key-${RUN_ID}-${ATTEMPT}"
TMP_WORKER_DIR="$(mktemp -d)"

cleanup() {
  set +e
  cd "$ROOT_DIR/producer-worker"
  npx wrangler delete "$WORKER_NAME" --force --config "$TMP_WORKER_DIR/wrangler.toml" >/dev/null 2>&1 || true
  npx wrangler queues delete "$JOBS_QUEUE" --force >/dev/null 2>&1 || true
  npx wrangler queues delete "$RESULTS_QUEUE" --force >/dev/null 2>&1 || true
  rm -rf "$TMP_WORKER_DIR"
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

cat > "$TMP_WORKER_DIR/wrangler.toml" <<TOML
name = "$WORKER_NAME"
main = "index.js"
compatibility_date = "2026-02-15"

[[queues.producers]]
queue = "$JOBS_QUEUE"
binding = "HPC_QUEUE"
TOML

cat > "$TMP_WORKER_DIR/index.js" <<JS
const API_KEY = ${API_KEY@Q};

function jsonResponse(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

export default {
  async fetch(request, env) {
    if (request.headers.get("x-api-key") !== API_KEY) {
      return jsonResponse({ error: "unauthorized" }, 401);
    }

    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/health") {
      return jsonResponse({ ok: true, service: "ci-e2e-worker" });
    }

    if (request.method === "POST" && url.pathname === "/jobs") {
      let payload;
      try {
        payload = await request.json();
      } catch {
        return jsonResponse({ error: "invalid_json" }, 400);
      }

      const job = {
        job_id: crypto.randomUUID(),
        input: payload?.input ?? {},
        created_at: new Date().toISOString(),
      };

      await env.HPC_QUEUE.send(job);

      return jsonResponse({ status: "queued", job_id: job.job_id, queue: "$JOBS_QUEUE" }, 202);
    }

    return jsonResponse({ error: "not_found" }, 404);
  },
};
JS

DEPLOY_LOG="$RUNTIME_DIR/ci-worker-deploy.log"
npx wrangler deploy --config "$TMP_WORKER_DIR/wrangler.toml" > "$DEPLOY_LOG" 2>&1
cat "$DEPLOY_LOG"

WORKER_URL="$(grep -Eo 'https://[^ ]+\.workers\.dev' "$DEPLOY_LOG" | head -n1 || true)"
if [[ -z "$WORKER_URL" ]]; then
  echo "failed to parse worker URL"
  exit 1
fi

RESP="$(curl -fsS -X POST "$WORKER_URL/jobs" \
  -H 'content-type: application/json' \
  -H "x-api-key: $API_KEY" \
  -d '{"input":{"iterations":3,"source":"github-actions-endpoint"}}')"

echo "$RESP" | grep -q '"status":"queued"'
JOB_ID="$(echo "$RESP" | python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["job_id"])')"

PYTHONUNBUFFERED=1 \
CF_QUEUES_API_TOKEN="$CLOUDFLARE_API_TOKEN" \
CF_ACCOUNT_ID="$CLOUDFLARE_ACCOUNT_ID" \
CF_JOBS_QUEUE_ID="$JOBS_QUEUE_ID" \
CF_RESULTS_QUEUE_ID="$RESULTS_QUEUE_ID" \
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
CF_RESULTS_QUEUE_ID="$RESULTS_QUEUE_ID" \
RESULTS_POLL_INTERVAL_SECONDS=1 \
python3 "$ROOT_DIR/laptop-consumer/laptop_pull_results.py" > "$RUNTIME_DIR/laptop.log" 2>&1 &
LAPTOP_PID=$!
sleep 8
kill "$LAPTOP_PID" >/dev/null 2>&1 || true
cat "$RUNTIME_DIR/laptop.log"

grep -q "$JOB_ID" "$RUNTIME_DIR/laptop.log"
test -f "$RUNTIME_DIR/results/$JOB_ID/output.json"

echo "full endpoint e2e ok for job $JOB_ID"
