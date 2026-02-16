# hpc_queue

Laptop submits jobs to `hpc-jobs`. HPC pulls jobs, computes, and enqueues result events to `hpc-results`. Laptop pulls results from `hpc-results`.

Architecture:
- Producer (Cloudflare Worker): receives `POST /jobs`, enqueues to `hpc-jobs`.
- Jobs Queue (`hpc-jobs`): buffers compute work.
- HPC Consumer: pulls from `hpc-jobs`, runs Apptainer directly on the compute node, enqueues `{job_id,status,result_pointer}` to `hpc-results`, then acks.
- Results Queue (`hpc-results`): buffers result events for laptop retrieval.
- Laptop Consumer: pulls from `hpc-results` and acks.

## Repository layout

- `producer-worker/`: Cloudflare Worker that enqueues jobs to `hpc-jobs`.
- `hpc-consumer/hpc_pull_consumer.py`: pull-based job consumer for HPC node.
- `laptop-consumer/laptop_pull_results.py`: pull-based result consumer for laptop.
- `.env.example`: environment variables for both components.

## 1) Prepare local tools

```bash
cd /Users/user/hpc_queue/producer-worker
npm install
```

## 2) Authenticate and create queue

```bash
npx wrangler login
npx wrangler queues create hpc-jobs
npx wrangler queues consumer http add hpc-jobs
npx wrangler queues create hpc-results
npx wrangler queues consumer http add hpc-results
```

Queue producer binding is already scaffolded in `/Users/user/hpc_queue/producer-worker/wrangler.toml`.

## 3) Deploy producer Worker

```bash
cd /Users/user/hpc_queue/producer-worker
npx wrangler secret put API_KEY
npx wrangler deploy
```

## 4) Configure HPC consumer environment

```bash
cd /Users/user/hpc_queue
cp .env.example .env
```

Set these required values in `.env`:
- `CF_ACCOUNT_ID`
- `CF_JOBS_QUEUE_ID`
- `CF_RESULTS_QUEUE_ID`
- `CF_QUEUES_API_TOKEN`
- `API_KEY`

## 5) Configure compute-node Apptainer runtime

Set these in `.env` on the compute node:
- `APPTAINER_IMAGE`: absolute path to your `.sif`
- `APPTAINER_OCI_REF`: OCI image reference in GHCR, for example `ghcr.io/<org>/hpc-queue-runtime:latest`
- `APPTAINER_BIN`: usually `apptainer`
- `APPTAINER_BIND`: extra bind mounts, for example `/scratch:/scratch`
- `CONTAINER_CMD`: command run inside container. It must read `/work/input.json` and write `/work/output.json`.

Each job gets a local working directory under `RESULTS_DIR/<job_id>/` and is mounted to `/work` inside the container.

## 6) Build runtime image with GitHub Actions (GHCR)

Workflow file:
- `/Users/user/hpc_queue/.github/workflows/container-build.yml`

On push to `main` (changes under `containers/`) or manual dispatch, it builds and pushes:
- `ghcr.io/<repo-owner>/hpc-queue-runtime:latest`
- `ghcr.io/<repo-owner>/hpc-queue-runtime:sha-<commit>`

## 7) Refresh local `.sif` from GHCR on compute node

```bash
cd /Users/user/hpc_queue
./hpc-consumer/scripts/update_apptainer_image.sh
```

## 8) Run HPC pull consumer (foreground)

```bash
cd /Users/user/hpc_queue
set -a
source .env
set +a
python3 ./hpc-consumer/hpc_pull_consumer.py
```

## 9) Run HPC pull consumer as auto-restarting service

```bash
cd /Users/user/hpc_queue
./hpc-consumer/install_user_service.sh
```

Service commands:

```bash
cd /Users/user/hpc_queue
./hpc-consumer/service_control.sh status
./hpc-consumer/service_control.sh logs
```

If it should survive logout/reboot for your user:

```bash
sudo loginctl enable-linger "$USER"
```

## 10) Run laptop result puller

```bash
cd /Users/user/hpc_queue
set -a
source .env
set +a
python3 ./laptop-consumer/laptop_pull_results.py
```

## 11) Submit a job from laptop

```bash
curl -X POST "https://<your-worker>.workers.dev/jobs" \
  -H "content-type: application/json" \
  -H "x-api-key: <API_KEY>" \
  -d '{
    "task": "simulate",
    "input": {"dataset": "s3://bucket/input.parquet", "iterations": 1000}
  }'
```

HPC output files are written to `hpc-consumer/results/<job_id>.json` by default. Result events appear in the laptop puller output.

## Notes

- Queue message bodies for pull consumers can be base64; this consumer decodes both base64 JSON and plain JSON.
