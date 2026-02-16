# hpc_queue

Single CLI: `q`

- Local submits jobs.
- HPC runs compute worker.
- Local pulls results.

## Keys

- `API_KEY`: auth for public submit endpoint (`/jobs`).
- `CF_QUEUES_API_TOKEN`: auth for Cloudflare Queue API (pull/ack/enqueue).

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

Submit raw JSON input instead:

```bash
q submit --json '{"command":"ls -la"}'
```

Pull one batch of result messages:

```bash
q results
```

View job logs on demand:

```bash
q logs <job_id>
```

`q logs` reads saved `stdout.log` and `stderr.log` from `hpc-consumer/results/<job_id>/`.

## HPC node

Start worker + auto image refresh + cron watchdog in one command:

```bash
q start
```

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
- If digest is unchanged, it skips pull.
- If digest changed, it pulls and updates the local `.sif`.

This gives both properties:
- never stale (digest checked every startup)
- no unnecessary re-pulls

The running HPC consumer also checks digest before each job execution, so long-running workers stay fresh without forced re-pulls.
