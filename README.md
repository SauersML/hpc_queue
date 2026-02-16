# hpc_queue

Queue-based HPC runner.

- Laptop submits jobs to `hpc-jobs`
- Compute node pulls jobs, runs Apptainer, writes results, enqueues status to `hpc-results`
- Laptop pulls result events from `hpc-results`

## 1) Start the producer API (already deployed in this repo)

Submit a job:

```bash
curl -X POST "https://hpc-queue-producer.sauer354.workers.dev/jobs" \
  -H "content-type: application/json" \
  -H "x-api-key: <API_KEY>" \
  -d '{"task":"simulate","input":{"iterations":100}}'
```

## 2) Configure `.env` on compute node and laptop

Required:

- `CF_ACCOUNT_ID`
- `CF_JOBS_QUEUE_ID`
- `CF_RESULTS_QUEUE_ID`
- `CF_QUEUES_API_TOKEN`
- `API_KEY`
- `APPTAINER_IMAGE`
- `APPTAINER_OCI_REF`

Use `/Users/user/hpc_queue/.env.example` as template.

## 3) Build container image (GitHub Actions)

Workflow: `/Users/user/hpc_queue/.github/workflows/container-build.yml`

- Push changes under `containers/` to `main`
- Actions builds and pushes to:
  - `ghcr.io/<owner>/hpc-queue-runtime:latest`
  - `ghcr.io/<owner>/hpc-queue-runtime:sha-<commit>`

## 4) Update `.sif` on compute node

```bash
cd /Users/user/hpc_queue
./hpc-consumer/scripts/update_apptainer_image.sh
```

## 5) Run compute-node worker (auto-restart)

```bash
cd /Users/user/hpc_queue
./hpc-consumer/install_user_service.sh
./hpc-consumer/service_control.sh status
./hpc-consumer/service_control.sh logs
```

Optional persistence across logout/reboot:

```bash
sudo loginctl enable-linger "$USER"
```

## 6) Run laptop results puller

```bash
cd /Users/user/hpc_queue
set -a; source .env; set +a
python3 ./laptop-consumer/laptop_pull_results.py
```

## Paths

- HPC worker: `/Users/user/hpc_queue/hpc-consumer/hpc_pull_consumer.py`
- Result puller: `/Users/user/hpc_queue/laptop-consumer/laptop_pull_results.py`
- Result files: `/Users/user/hpc_queue/hpc-consumer/results/<job_id>/output.json`
