# hpc_queue

Queue-based HPC runner.

- Laptop submits jobs to `hpc-jobs`
- Compute node pulls jobs, runs Apptainer, writes results, enqueues status to `hpc-results`
- Laptop pulls result events from `hpc-results`

## What users must set

Only two values in `.env`:

- `CF_QUEUES_API_TOKEN`
- `API_KEY`

Everything else is hardcoded with sane defaults.

Use template:

```bash
cp /Users/user/hpc_queue/.env.example /Users/user/hpc_queue/.env
```

## Submit a job

```bash
curl -X POST "https://hpc-queue-producer.sauer354.workers.dev/jobs" \
  -H "content-type: application/json" \
  -H "x-api-key: <API_KEY>" \
  -d '{"task":"simulate","input":{"iterations":100}}'
```

## Update local container image on compute node

```bash
cd /Users/user/hpc_queue
./hpc-consumer/scripts/update_apptainer_image.sh
```

## Run compute-node worker (auto-restart)

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

## Run laptop results puller

```bash
cd /Users/user/hpc_queue
set -a; source .env; set +a
python3 ./laptop-consumer/laptop_pull_results.py
```
