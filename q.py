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
RESULTS_CACHE_PATH = ROOT / "local-consumer" / "results_cache.jsonl"
LOCAL_RESULTS_DIR = ROOT / "local-results"

DEFAULT_WORKER_URL = "https://hpc-queue-producer.sauer354.workers.dev"
DEFAULT_CF_ACCOUNT_ID = "59908b351c3a3321ff84dd2d78bf0b42"
DEFAULT_CF_RESULTS_QUEUE_ID = "a435ae20f7514ce4b193879704b03e4e"


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


def cmd_login(queue_token: str | None, api_key: str | None) -> None:
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


def results_api_base() -> str:
    account_id = os.getenv("CF_ACCOUNT_ID", DEFAULT_CF_ACCOUNT_ID)
    results_queue_id = os.getenv("CF_RESULTS_QUEUE_ID", DEFAULT_CF_RESULTS_QUEUE_ID)
    return (
        "https://api.cloudflare.com/client/v4/accounts/"
        f"{account_id}/queues/{results_queue_id}/messages"
    )


def cf_post(url: str, token: str, payload: dict[str, Any]) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    req = request.Request(
        url=url,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    with request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def parse_result_messages(resp: dict[str, Any]) -> list[dict[str, Any]]:
    result = resp.get("result", {})
    if isinstance(result, dict):
        return result.get("messages", [])
    if isinstance(result, list):
        return result
    return []


def cache_result_event(event: dict[str, Any]) -> None:
    RESULTS_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with RESULTS_CACHE_PATH.open("a", encoding="utf-8") as cache_fp:
        cache_fp.write(json.dumps(event, separators=(",", ":")) + "\n")


def write_local_result_file(event: dict[str, Any]) -> Path | None:
    job_id = str(event.get("job_id", "")).strip()
    status = str(event.get("status", "")).strip()
    if not job_id or status not in {"completed", "failed"}:
        return None
    LOCAL_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = LOCAL_RESULTS_DIR / f"{job_id}.json"
    out_path.write_text(json.dumps(event, indent=2), encoding="utf-8")
    return out_path


def pull_results_once(queue_token: str) -> list[dict[str, Any]]:
    pulled = cf_post(
        url=f"{results_api_base()}/pull",
        token=queue_token,
        payload={
            "batch_size": int(os.getenv("RESULTS_BATCH_SIZE", "10")),
            "visibility_timeout": int(os.getenv("RESULTS_VISIBILITY_TIMEOUT_MS", "120000")),
        },
    )
    messages = parse_result_messages(pulled)
    if not messages:
        return []

    acks: list[dict[str, str]] = []
    events: list[dict[str, Any]] = []
    for message in messages:
        lease_id = message.get("lease_id")
        if not lease_id:
            continue
        body = message.get("body")
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except Exception:
                body = {"raw": body}
        if isinstance(body, dict):
            events.append(body)
            cache_result_event(body)
            write_local_result_file(body)
        acks.append({"lease_id": lease_id})

    if acks:
        cf_post(
            url=f"{results_api_base()}/ack",
            token=queue_token,
            payload={"acks": acks, "retries": []},
        )
    return events


def wait_for_job_result(job_id: str, queue_token: str) -> Path:
    poll_seconds = float(os.getenv("RESULTS_POLL_INTERVAL_SECONDS", "2"))
    last_heartbeat = time.monotonic()
    while True:
        events = pull_results_once(queue_token)
        for event in events:
            if str(event.get("job_id")) != job_id:
                continue
            status = str(event.get("status", ""))
            if status in {"completed", "failed"}:
                out_path = write_local_result_file(event)
                if out_path is None:
                    raise RuntimeError("internal error: missing result file path")
                print(json.dumps(event, indent=2))
                print(f"local_result_file: {out_path}")
                return out_path

        now = time.monotonic()
        if now - last_heartbeat >= 30:
            print(f"waiting for job {job_id} ...")
            last_heartbeat = now
        time.sleep(poll_seconds)


def cmd_submit(raw_parts: list[str], no_wait: bool) -> None:
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
            # Workers.dev may block default urllib signature on some networks.
            "user-agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            ),
            "accept": "application/json",
            "accept-language": "en-US,en;q=0.9",
        },
    )

    try:
        with request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        print(json.dumps(body, indent=2))
        if no_wait:
            return
        job_id = str(body.get("job_id", "")).strip()
        if not job_id:
            raise RuntimeError("submit succeeded but missing job_id")
        queue_token = require_env("CF_QUEUES_API_TOKEN")
        wait_for_job_result(job_id=job_id, queue_token=queue_token)
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"submit failed: HTTP {exc.code}: {detail}") from exc


def maybe_refresh_image() -> None:
    print("refreshing Apptainer image...")
    try:
        run([str(ROOT / "hpc-consumer" / "scripts" / "update_apptainer_image.sh")], cwd=ROOT)
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            "image refresh failed. verify APPTAINER_SIF_URL/APPTAINER_SIF_SHA256_URL are reachable."
        ) from exc


def cmd_worker() -> None:
    require_env("CF_QUEUES_API_TOKEN")
    maybe_refresh_image()

    run([str(ROOT / "hpc-consumer" / "start_consumer.sh")], cwd=ROOT)
    print("worker started")
    print(f"log file: {ROOT / 'hpc-consumer' / 'hpc_pull_consumer.log'}")


def cmd_results() -> None:
    require_env("CF_QUEUES_API_TOKEN")
    run([sys.executable, str(ROOT / "local-consumer" / "local_pull_results.py")], cwd=ROOT)


def cmd_logs(job_id: str) -> None:
    job_dir = ROOT / "hpc-consumer" / "results" / job_id
    meta_path = job_dir / "meta.json"
    stdout_path = job_dir / "stdout.log"
    stderr_path = job_dir / "stderr.log"

    if not job_dir.exists():
        if RESULTS_CACHE_PATH.exists():
            last_match: dict[str, Any] | None = None
            for line in RESULTS_CACHE_PATH.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except Exception:
                    continue
                if isinstance(event, dict) and str(event.get("job_id")) == job_id:
                    last_match = event
            if last_match is not None:
                print(
                    json.dumps(
                        {
                            "job_id": last_match.get("job_id"),
                            "status": last_match.get("status"),
                            "exit_code": last_match.get("exit_code"),
                            "started_at": last_match.get("started_at"),
                            "finished_at": last_match.get("finished_at"),
                            "result_pointer": last_match.get("result_pointer"),
                            "source": "results_cache",
                        },
                        indent=2,
                    )
                )
                print("\n=== stdout ===")
                print(str(last_match.get("stdout_tail", "")), end="")
                print("\n=== stderr ===")
                print(str(last_match.get("stderr_tail", "")), end="")
                print()
                return

        raise RuntimeError(
            f"No local results for job_id={job_id} at {job_dir}. "
            f"Run `q results` first or check logs on HPC."
        )

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

    print(json.dumps({"running": running, "pid": pid or None}))


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
        "--no-wait",
        action="store_true",
        help="queue job and return immediately (default waits for completion)",
    )
    submit.add_argument(
        "payload",
        nargs=argparse.REMAINDER,
        help="shell command to run inside the container",
    )

    login = sub.add_parser("login", help="configure local .env")
    login.add_argument("--queue-token", help="queue-token for Cloudflare Queue API")
    login.add_argument("--api-key", help="api-key for /jobs auth; auto-generated if omitted")
    sub.add_parser("start", help="start compute worker")
    sub.add_parser("worker", help="deprecated alias for start")
    sub.add_parser("results", help="pull one batch of results on local machine")
    logs = sub.add_parser("logs", help="show stdout/stderr for a completed job")
    logs.add_argument("job_id", help="job id to inspect from local hpc-consumer/results")
    sub.add_parser("status", help="show worker status")
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
        cmd_submit(args.payload, args.no_wait)
    elif args.command == "login":
        cmd_login(
            queue_token=args.queue_token,
            api_key=args.api_key,
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
