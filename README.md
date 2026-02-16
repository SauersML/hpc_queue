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
mkdir -p "$HOME/.local/bin"
ln -sf /Users/user/hpc_queue/q.py "$HOME/.local/bin/q"
chmod +x /Users/user/hpc_queue/q.py
export PATH="$HOME/.local/bin:$PATH"
```

If needed, add this to your shell profile (`~/.zshrc` or `~/.bashrc`):

```bash
export PATH="$HOME/.local/bin:$PATH"
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

Pull results and live logs:

```bash
q results
```

`q results` prints streaming log events (`status=running`) and final completion message with `result_pointer`.

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
