#!/usr/bin/env python3
from __future__ import annotations

import argparse
import getpass
import json
import os
import secrets
import subprocess
import sys
from pathlib import Path
from typing import Any
from urllib import error, request

ROOT = Path(__file__).resolve().parent
ENV_PATH = ROOT / ".env"
PID_FILE = ROOT / "hpc-consumer" / "hpc_pull_consumer.pid"
CRON_TAG = "HPC_QUEUE_WATCHDOG"
DEFAULT_APPTAINER_IMAGE = str(ROOT / "runtime" / "hpc-queue-runtime.sif")

DEFAULT_WORKER_URL = "https://hpc-queue-producer.sauer354.workers.dev"


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def require_env(name: str) -> str:
    value = os.getenv(name, "")
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def run(cmd: list[str], cwd: Path | None = None) -> None:
    env = os.environ.copy()
    env.setdefault("PYTHON_BIN", sys.executable)
    subprocess.run(cmd, cwd=str(cwd) if cwd else None, check=True, env=env)


def upsert_env(path: Path, updates: dict[str, str]) -> None:
    lines: list[str] = []
    if path.exists():
        lines = path.read_text(encoding="utf-8").splitlines()

    seen: set[str] = set()
    out: list[str] = []
    for line in lines:
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in line:
            out.append(line)
            continue
        key = line.split("=", 1)[0].strip()
        if key in updates:
            out.append(f"{key}={updates[key]}")
            seen.add(key)
        else:
            out.append(line)

    for key, value in updates.items():
        if key not in seen:
            out.append(f"{key}={value}")

    path.write_text("\n".join(out).rstrip() + "\n", encoding="utf-8")


def cmd_login(
    queue_token: str | None,
    api_key: str | None,
    ghcr_token: str | None,
    ghcr_username: str | None,
) -> None:
    existing_queue_token = os.getenv("CF_QUEUES_API_TOKEN", "")
    existing_api_key = os.getenv("API_KEY", "")

    final_queue_token = queue_token or existing_queue_token
    if not final_queue_token:
        final_queue_token = getpass.getpass("queue-token (Cloudflare Queue API token): ").strip()
    if not final_queue_token:
        raise RuntimeError("CF_QUEUES_API_TOKEN cannot be empty")

    final_api_key = api_key or existing_api_key
    generated = False
    if not final_api_key:
        final_api_key = secrets.token_hex(24)
        generated = True

    worker_url = os.getenv("WORKER_URL", DEFAULT_WORKER_URL)
    updates = {
        "CF_QUEUES_API_TOKEN": final_queue_token,
        "API_KEY": final_api_key,
        "WORKER_URL": worker_url,
        "PYTHON_BIN": sys.executable,
    }
    if ghcr_token:
        updates["GHCR_TOKEN"] = ghcr_token
    if ghcr_username:
        updates["GHCR_USERNAME"] = ghcr_username
    upsert_env(ENV_PATH, updates)
    load_dotenv(ENV_PATH)

    print("login configuration saved to .env")
    if generated:
        print(f"generated api-key: {final_api_key}")
    else:
        print("api-key: kept existing/provided value")


def build_submit_input(raw_parts: list[str]) -> dict[str, Any]:
    if not raw_parts:
        raise RuntimeError("submit requires a command")
    raw = " ".join(raw_parts).strip()
    return {"command": raw}


def cmd_submit(raw_parts: list[str]) -> None:
    api_key = require_env("API_KEY")
    worker_url = os.getenv("WORKER_URL", DEFAULT_WORKER_URL).rstrip("/")
    payload = {"input": build_submit_input(raw_parts)}

    req = request.Request(
        url=f"{worker_url}/jobs",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "content-type": "application/json",
            "x-api-key": api_key,
        },
    )

    try:
        with request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        print(json.dumps(body, indent=2))
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"submit failed: HTTP {exc.code}: {detail}") from exc


def get_apptainer_image_path() -> Path:
    return Path(os.getenv("APPTAINER_IMAGE", DEFAULT_APPTAINER_IMAGE)).expanduser()


def prompt_ghcr_credentials() -> bool:
    token = getpass.getpass("ghcr-token (for first image pull): ").strip()
    if not token:
        return False
    username = input("ghcr-username (optional, press Enter to skip): ").strip()
    updates = {"GHCR_TOKEN": token}
    if username:
        updates["GHCR_USERNAME"] = username
    upsert_env(ENV_PATH, updates)
    load_dotenv(ENV_PATH)
    return True


def maybe_refresh_image() -> None:
    print("refreshing Apptainer image...")
    try:
        run([str(ROOT / "hpc-consumer" / "scripts" / "update_apptainer_image.sh")], cwd=ROOT)
    except subprocess.CalledProcessError as exc:
        image_path = get_apptainer_image_path()
        if not image_path.exists() and not os.getenv("GHCR_TOKEN") and sys.stdin.isatty():
            print("no local image found; ghcr auth is required for first pull")
            if prompt_ghcr_credentials():
                run([str(ROOT / "hpc-consumer" / "scripts" / "update_apptainer_image.sh")], cwd=ROOT)
                return
        raise RuntimeError(
            "image refresh failed. If your GHCR image is private, set GHCR_TOKEN in .env and rerun `q start`."
        ) from exc


def cmd_worker() -> None:
    require_env("CF_QUEUES_API_TOKEN")
    maybe_refresh_image()

    run([str(ROOT / "hpc-consumer" / "start_consumer.sh")], cwd=ROOT)
    run([str(ROOT / "hpc-consumer" / "install_cron_watchdog.sh")], cwd=ROOT)

    print("worker started and cron watchdog installed")
    print(f"log file: {ROOT / 'hpc-consumer' / 'hpc_pull_consumer.log'}")


def cmd_results() -> None:
    require_env("CF_QUEUES_API_TOKEN")
    run([sys.executable, str(ROOT / "laptop-consumer" / "laptop_pull_results.py")], cwd=ROOT)


def cmd_logs(job_id: str) -> None:
    job_dir = ROOT / "hpc-consumer" / "results" / job_id
    meta_path = job_dir / "meta.json"
    stdout_path = job_dir / "stdout.log"
    stderr_path = job_dir / "stderr.log"

    if not job_dir.exists():
        raise RuntimeError(f"No local results for job_id={job_id} at {job_dir}")

    if meta_path.exists():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        print(json.dumps(
            {
                "job_id": meta.get("job_id"),
                "status": meta.get("status"),
                "returncode": meta.get("returncode"),
                "started_at": meta.get("started_at"),
                "finished_at": meta.get("finished_at"),
            },
            indent=2,
        ))
    else:
        print(json.dumps({"job_id": job_id, "warning": "meta.json not found"}, indent=2))

    if stdout_path.exists():
        print("\n=== stdout ===")
        print(stdout_path.read_text(encoding="utf-8"), end="")
    else:
        print("\n=== stdout ===")
        print("(missing)")

    if stderr_path.exists():
        print("\n=== stderr ===")
        print(stderr_path.read_text(encoding="utf-8"), end="")
    else:
        print("\n=== stderr ===")
        print("(missing)")


def cmd_status() -> None:
    running = False
    pid = ""
    if PID_FILE.exists():
        pid = PID_FILE.read_text(encoding="utf-8").strip()
        if pid:
            proc = subprocess.run(["kill", "-0", pid], capture_output=True)
            running = proc.returncode == 0

    cron = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    cron_enabled = CRON_TAG in (cron.stdout or "")

    print(json.dumps({"running": running, "pid": pid or None, "cron_watchdog": cron_enabled}))


def cmd_stop() -> None:
    if PID_FILE.exists():
        pid = PID_FILE.read_text(encoding="utf-8").strip()
        if pid:
            subprocess.run(["kill", pid], check=False)
    print("stop signal sent")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="hpc_queue control CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    submit = sub.add_parser("submit", help="submit a shell command job")
    submit.add_argument(
        "payload",
        nargs=argparse.REMAINDER,
        help="shell command to run inside the container",
    )

    login = sub.add_parser("login", help="configure local .env")
    login.add_argument("--queue-token", help="queue-token for Cloudflare Queue API")
    login.add_argument("--api-key", help="api-key for /jobs auth; auto-generated if omitted")
    login.add_argument("--ghcr-token", help="ghcr-token for pulling private runtime image")
    login.add_argument("--ghcr-username", help="ghcr username for the token (optional)")
    sub.add_parser("start", help="start compute worker and install cron watchdog")
    sub.add_parser("worker", help="deprecated alias for start")
    sub.add_parser("results", help="pull one batch of results on local machine")
    logs = sub.add_parser("logs", help="show stdout/stderr for a completed job")
    logs.add_argument("job_id", help="job id to inspect from local hpc-consumer/results")
    sub.add_parser("status", help="show worker/cron status")
    sub.add_parser("stop", help="stop worker process")

    return parser


def main() -> None:
    load_dotenv(ENV_PATH)
    parser = build_parser()
    known_commands = {"submit", "login", "start", "worker", "results", "logs", "status", "stop"}
    argv = sys.argv[1:]
    if argv and argv[0] not in known_commands and not argv[0].startswith("-"):
        # Shorthand: `q.py <command...>` behaves like `q.py submit <command...>`.
        argv = ["submit", *argv]
    args = parser.parse_args(argv)

    if args.command == "submit":
        cmd_submit(args.payload)
    elif args.command == "login":
        cmd_login(
            queue_token=args.queue_token,
            api_key=args.api_key,
            ghcr_token=args.ghcr_token,
            ghcr_username=args.ghcr_username,
        )
    elif args.command in {"start", "worker"}:
        cmd_worker()
    elif args.command == "results":
        cmd_results()
    elif args.command == "logs":
        cmd_logs(args.job_id)
    elif args.command == "status":
        cmd_status()
    elif args.command == "stop":
        cmd_stop()
    else:
        parser.error("unknown command")


if __name__ == "__main__":
    main()
