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

Submit a command job:

```bash
python3 /Users/user/hpc_queue/q.py submit ls
python3 /Users/user/hpc_queue/q.py submit "python /work/script.py --iters 100"
python3 /Users/user/hpc_queue/q.py python3 /work/thisfile.py
```

Submit raw JSON input instead:

```bash
python3 /Users/user/hpc_queue/q.py submit --json '{"command":"ls -la"}'
```

Pull results and live logs:

```bash
python3 /Users/user/hpc_queue/q.py results
```

`q.py results` prints streaming log events (`status=running`) and final completion message with `result_pointer`.

## HPC node

Start worker + auto image refresh + cron watchdog in one command:

```bash
python3 /Users/user/hpc_queue/q.py start
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

`q.py start` resolves the remote image digest first.
- If digest is unchanged, it skips pull.
- If digest changed, it pulls and updates the local `.sif`.

This gives both properties:
- never stale (digest checked every startup)
- no unnecessary re-pulls

The running HPC consumer also checks digest before each job execution, so long-running workers stay fresh without forced re-pulls.
