#!/usr/bin/env python3
from __future__ import annotations

import argparse
import getpass
import json
import os
import secrets
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from urllib import error, request

ROOT = Path(__file__).resolve().parent
ENV_PATH = ROOT / ".env"
PID_FILE = ROOT / "hpc-consumer" / "hpc_pull_consumer.pid"
CRON_TAG = "HPC_QUEUE_WATCHDOG"

DEFAULT_WORKER_URL = "https://hpc-queue-producer.sauer354.workers.dev"
DEFAULT_IMAGE_REFRESH_HOURS = 12


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
    subprocess.run(cmd, cwd=str(cwd) if cwd else None, check=True)


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


def cmd_login(queue_token: str | None, api_key: str | None, worker_url: str) -> None:
    existing_queue_token = os.getenv("CF_QUEUES_API_TOKEN", "")
    existing_api_key = os.getenv("API_KEY", "")

    final_queue_token = queue_token or existing_queue_token
    if not final_queue_token:
        final_queue_token = getpass.getpass("Cloudflare queue token (HPC_QUEUE_KEY): ").strip()
    if not final_queue_token:
        raise RuntimeError("CF_QUEUES_API_TOKEN cannot be empty")

    final_api_key = api_key or existing_api_key
    generated = False
    if not final_api_key:
        final_api_key = secrets.token_hex(24)
        generated = True

    updates = {
        "CF_QUEUES_API_TOKEN": final_queue_token,
        "API_KEY": final_api_key,
        "WORKER_URL": worker_url,
    }
    upsert_env(ENV_PATH, updates)
    load_dotenv(ENV_PATH)

    print("login configuration saved to .env")
    if generated:
        print(f"generated API_KEY: {final_api_key}")
    else:
        print("API_KEY: kept existing/provided value")


def parse_input(raw: str) -> dict[str, Any]:
    if raw.startswith("@"):
        file_path = Path(raw[1:]).expanduser().resolve()
        payload = json.loads(file_path.read_text(encoding="utf-8"))
    else:
        payload = json.loads(raw)

    if not isinstance(payload, dict):
        raise RuntimeError("input must be a JSON object")
    return payload


def cmd_submit(raw_input: str) -> None:
    api_key = require_env("API_KEY")
    worker_url = os.getenv("WORKER_URL", DEFAULT_WORKER_URL).rstrip("/")
    payload = {"input": parse_input(raw_input)}

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


def maybe_refresh_image() -> None:
    image_path = Path(
        os.getenv("APPTAINER_IMAGE", "/Users/user/hpc_queue/runtime/hpc-queue-runtime.sif")
    )
    refresh_hours = int(os.getenv("IMAGE_REFRESH_HOURS", str(DEFAULT_IMAGE_REFRESH_HOURS)))

    needs_update = not image_path.exists()
    if image_path.exists():
        age_seconds = time.time() - image_path.stat().st_mtime
        needs_update = age_seconds > refresh_hours * 3600

    if needs_update:
        print("refreshing Apptainer image...")
        run([str(ROOT / "hpc-consumer" / "scripts" / "update_apptainer_image.sh")], cwd=ROOT)
    else:
        print("Apptainer image is fresh; skipping update")


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

    submit = sub.add_parser("submit", help="submit a job input JSON")
    submit.add_argument("input", help="JSON object or @/path/to/input.json")

    login = sub.add_parser("login", help="configure local .env")
    login.add_argument("--queue-token", help="Cloudflare queue API token (HPC_QUEUE_KEY)")
    login.add_argument("--api-key", help="Worker API key for /jobs auth; auto-generated if omitted")
    login.add_argument("--worker-url", default=DEFAULT_WORKER_URL, help="Worker base URL")
    sub.add_parser("worker", help="start compute worker and install cron watchdog")
    sub.add_parser("results", help="pull results on local machine")
    sub.add_parser("status", help="show worker/cron status")
    sub.add_parser("stop", help="stop worker process")

    return parser


def main() -> None:
    load_dotenv(ENV_PATH)
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "submit":
        cmd_submit(args.input)
    elif args.command == "login":
        cmd_login(
            queue_token=args.queue_token,
            api_key=args.api_key,
            worker_url=args.worker_url,
        )
    elif args.command == "worker":
        cmd_worker()
    elif args.command == "results":
        cmd_results()
    elif args.command == "status":
        cmd_status()
    elif args.command == "stop":
        cmd_stop()
    else:
        parser.error("unknown command")


if __name__ == "__main__":
    main()
