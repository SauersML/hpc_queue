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

Stop worker:

```bash
q stop
```

## How worker image updates happen

`q start` resolves the remote image digest first.
- If digest is unchanged, it skips download.
- If digest changed, it downloads the new `.sif` built by GitHub Actions and updates local image.

This gives both properties:
- never stale (digest checked every startup)
- no unnecessary re-pulls

The running HPC consumer also checks digest before each job execution, so long-running workers stay fresh without forced re-pulls.
