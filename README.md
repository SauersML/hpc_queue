# hpc_queue

Single CLI: `q`

- Local submits jobs.
- HPC runs compute worker.
- Local pulls results.

## Keys

- `api-key`: auth for public submit endpoint (`/jobs`), stored as `API_KEY`.
- `queue-token`: auth for Cloudflare Queue API (pull/ack/enqueue), stored as `CF_QUEUES_API_TOKEN`.

## Setup

Install `q` command:

```bash
curl -fsSL https://raw.githubusercontent.com/SauersML/hpc_queue/main/install.sh | bash
```

Run one command to configure:

```bash
q login
```

## Local machine

Submit a command job:

```bash
q submit ls
q submit "python /work/script.py --iters 100"
q python3 /work/thisfile.py
```

Submit a host command job (outside Apptainer on HPC node):

```bash
q host "nproc"
q host "top -n 1"
```

Run a local file on HPC (file is uploaded into the job payload):

```bash
q run-file ./script.py
q run-file ./script.py -- --arg1 10 --arg2 test
```

By default `q run-file` executes with `python`.
Set a different runner with `--runner`, for example:

```bash
q run-file --runner bash ./job.sh -- --fast
```

`q submit` now returns immediately by default.
It starts a background local results watcher and writes:
- `local-results/<job_id>.json`
- `local-results/<job_id>.stdout.log`
- `local-results/<job_id>.stderr.log`

Use blocking mode only when needed:

```bash
q submit --wait ls
```

Pull one batch of result messages:

```bash
q results
```

`q results` also writes `local-results/<job_id>.json` for completed jobs.

View job logs on demand:

```bash
q logs <job_id>
```

`q logs` reads local files when available, and otherwise falls back to cached queue result tails.

## CLI Reference

`q login`
- `--queue-token <token>`: set Cloudflare queue API token.
- `--api-key <key>`: set `/jobs` API key (auto-generated if omitted).

`q submit <command...>`
- Runs command inside Apptainer runtime on HPC.
- `--wait`: block until local result file exists, then print logs.

`q host <command...>`
- Runs command directly on HPC host (outside container).
- `--wait`: block until local result file exists, then print logs.

`q run-file [--runner <bin>] <local_file> [-- <args...>]`
- Uploads local file into job payload and executes it on HPC in container.
- Default runner is `python`.
- `--runner bash` for shell scripts, or `--runner ""` to execute directly.
- `--wait`: block until local result file exists, then print logs.

`q logs <job_id>`
- Prints job summary plus stdout/stderr from local artifacts or cache.

`q results`
- Pull one batch from results queue and write local artifacts.

`q status`
- Shows local process status.
- On local, also includes remote heartbeat fields:
  - `hpc_running_remote`
  - `hpc_last_heartbeat`
  - `hpc_heartbeat_age_seconds`
- Default output is human-readable text.
- Use `q status --json` for raw JSON output.

`q clear jobs|results|all`
- Purges messages from selected queue(s) using pull+ack loops.
- `--batch-size <n>`: pull size per cycle (default `100`).
- `--max-batches <n>`: max cycles (default `200`).

`q stop`
- Stops local worker/watcher processes.
- `--all`: also clears jobs + results queues.

## HPC node

Start worker + auto image refresh in one command:

```bash
q start
```

If `q start` fails, verify `apptainer` is installed and `APPTAINER_SIF_URL` is reachable.

Check status:

```bash
q status
```

`q status` on local also reports `hpc_running_remote` using heartbeat events from the HPC consumer.

Stop worker:

```bash
q stop
```

Stop local processes and clear both queues:

```bash
q stop --all
```

Clear queues directly:

```bash
q clear jobs
q clear results
q clear all
```

## How worker image updates happen

`q start` resolves the remote image digest first.
- If digest is unchanged, it skips download.
- If digest changed, it downloads the new `.sif` built by GitHub Actions and updates local image.

This gives both properties:
- never stale (digest checked every startup)
- no unnecessary re-pulls

The running HPC consumer also checks digest before each job execution, so long-running workers stay fresh without forced re-pulls.

## Runtime image contents

The default runtime image now includes:
- Python 3.12 + common data/biology Python packages from `containers/requirements.txt`
- Rust toolchain (`rustup` stable)
- Standard dev tools (`build-essential`, `git`, `curl`, `jq`, `vim`, `htop`, etc.)
- `bcftools`
- `plink2` and `gctb` (via micromamba/bioconda)
- Cloned repos:
  - `/gnomon`
  - `/reagle`

## Repo freshness (`gnomon` / `reagle`)

For container jobs, the HPC consumer now syncs these repos before each job and bind-mounts them:
- `/gnomon`
- `/reagle`

This means jobs see latest refs (default `main`) without waiting for a container rebuild.

Optional env overrides (on HPC):
- `SYNC_EXTERNAL_REPOS=1|0` (default `1`)
- `EXTERNAL_REPO_SYNC_STRICT=1|0` (default `1`)
- `GNOMON_REPO_URL`, `GNOMON_REPO_REF`
- `REAGLE_REPO_URL`, `REAGLE_REPO_REF`
