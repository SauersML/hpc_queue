# hpc_queue

Single CLI: `q.py`

- Local submits jobs.
- HPC runs compute worker.
- Local pulls results.

## Keys

- `API_KEY`: auth for public submit endpoint (`/jobs`).
- `CF_QUEUES_API_TOKEN`: auth for Cloudflare Queue API (pull/ack/enqueue).

## Setup

Run one command to configure:

```bash
python3 /Users/user/hpc_queue/q.py login
```

## Local machine

Submit a job (JSON object or `@file.json`):

```bash
python3 /Users/user/hpc_queue/q.py submit '{"iterations":100}'
```

Pull results:

```bash
python3 /Users/user/hpc_queue/q.py results
```

## HPC node

Start worker + auto image refresh + cron watchdog in one command:

```bash
python3 /Users/user/hpc_queue/q.py worker
```

Check status:

```bash
python3 /Users/user/hpc_queue/q.py status
```

Stop worker:

```bash
python3 /Users/user/hpc_queue/q.py stop
```

## How worker image updates happen

`q.py worker` resolves the remote image digest first.
- If digest is unchanged, it skips pull.
- If digest changed, it pulls and updates the local `.sif`.

This gives both properties:
- never stale (digest checked every startup)
- no unnecessary re-pulls
