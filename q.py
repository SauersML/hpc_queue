#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import getpass
import json
import os
import re
import secrets
import shlex
import stat
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from urllib import error, request
from urllib.parse import quote

ROOT = Path(__file__).resolve().parent
ENV_PATH = ROOT / ".env"
PID_FILE = ROOT / "hpc-consumer" / "hpc_supervisor.pid"
WORKER_PID_FILE = ROOT / "hpc-consumer" / "hpc_pull_consumer.pid"
RESULTS_CACHE_PATH = ROOT / "local-consumer" / "results_cache.jsonl"
LOCAL_RESULTS_DIR = ROOT / "local-results"
LOCAL_WATCHER_PID_FILE = ROOT / "local-consumer" / "local_results_watcher.pid"
LOCAL_WATCHER_LOG_FILE = ROOT / "local-consumer" / "local_results_watcher.log"
HPC_STATUS_PATH = ROOT / "local-consumer" / "hpc_status.json"

DEFAULT_WORKER_URL = "https://hpc-queue-producer.sauer354.workers.dev"
DEFAULT_INLINE_FILE_MAX_BYTES = 64 * 1024
DEFAULT_CF_ACCOUNT_ID = "59908b351c3a3321ff84dd2d78bf0b42"
DEFAULT_CF_JOBS_QUEUE_ID = "f52e2e6bb569425894ede9141e9343a5"
DEFAULT_CF_RESULTS_QUEUE_ID = "a435ae20f7514ce4b193879704b03e4e"
DEFAULT_RESULTS_POLL_INTERVAL_SECONDS = 2.0
DEFAULT_HPC_HEARTBEAT_MAX_AGE_SECONDS = 90.0
REPO_URL = "https://github.com/SauersML/hpc_queue.git"


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


def cf_post(url: str, token: str, payload: dict[str, Any]) -> dict[str, Any]:
    req = request.Request(
        url=url,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    with request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def parse_messages(resp: dict[str, Any]) -> list[dict[str, Any]]:
    result = resp.get("result", {})
    if isinstance(result, dict):
        msgs = result.get("messages", [])
        return msgs if isinstance(msgs, list) else []
    if isinstance(result, list):
        return result
    return []


def run(cmd: list[str], cwd: Path | None = None) -> None:
    env = os.environ.copy()
    env.setdefault("PYTHON_BIN", sys.executable)
    subprocess.run(cmd, cwd=str(cwd) if cwd else None, check=True, env=env)


def process_matches(pid: str, needle: str) -> bool:
    if not pid:
        return False
    probe = subprocess.run(["kill", "-0", pid], capture_output=True)
    if probe.returncode != 0:
        return False
    ps = subprocess.run(["ps", "-p", pid, "-o", "command="], capture_output=True, text=True)
    if ps.returncode != 0:
        return False
    return needle in (ps.stdout or "")


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

    updates = {
        "CF_QUEUES_API_TOKEN": final_queue_token,
        "API_KEY": final_api_key,
    }
    upsert_env(ENV_PATH, updates)
    load_dotenv(ENV_PATH)

    print("login configuration saved to .env")
    if generated:
        print(f"generated api-key: {final_api_key}")
    else:
        print("api-key: kept existing/provided value")


def normalize_python311_command(command: str) -> tuple[str, bool]:
    normalized = re.sub(r"(?<![\w./-])python3\.11(?![\w.-])", "python", command)
    return normalized, normalized != command


def build_submit_input(raw_parts: list[str], exec_mode: str) -> tuple[dict[str, Any], bool]:
    if not raw_parts:
        raise RuntimeError("submit requires a command")
    # Preserve original argv argument boundaries (critical for forms like: bash -lc '...').
    raw = shlex.join(raw_parts).strip()
    normalized, rewritten = normalize_python311_command(raw)
    return {"command": normalized, "exec_mode": exec_mode}, rewritten


def build_run_file_input(
    file_path: str,
    file_args: list[str],
    exec_mode: str,
    runner: str,
) -> dict[str, Any]:
    source = Path(file_path).expanduser().resolve()
    if not source.exists() or not source.is_file():
        raise RuntimeError(f"run-file source does not exist or is not a file: {source}")

    max_bytes = DEFAULT_INLINE_FILE_MAX_BYTES
    raw = source.read_bytes()
    if len(raw) > max_bytes:
        raise RuntimeError(
            f"run-file too large ({len(raw)} bytes). max allowed is {max_bytes} bytes."
        )

    mode_bits = stat.S_IMODE(source.stat().st_mode)
    mode = "755" if mode_bits & stat.S_IXUSR else "644"
    remote_rel = f"files/{source.name}"
    remote_abs = f"/work/{remote_rel}"

    normalized_args = list(file_args)
    if normalized_args and normalized_args[0] == "--":
        normalized_args = normalized_args[1:]

    cmd_parts: list[str] = []
    if runner:
        cmd_parts.append(shlex.quote(runner))
    cmd_parts.append(shlex.quote(remote_abs))
    cmd_parts.extend(shlex.quote(arg) for arg in normalized_args)
    command = " ".join(cmd_parts)

    return {
        "command": command,
        "exec_mode": exec_mode,
        "local_files": [
            {
                "path": remote_rel,
                "content_b64": base64.b64encode(raw).decode("ascii"),
                "mode": mode,
            }
        ],
    }


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
    record = dict(event)
    stdout_tail = str(record.pop("stdout_tail", ""))
    stderr_tail = str(record.pop("stderr_tail", ""))
    stdout_path = LOCAL_RESULTS_DIR / f"{job_id}.stdout.log"
    stdout_path.write_text(stdout_tail, encoding="utf-8")
    record["stdout_tail_file"] = str(stdout_path)
    stderr_path = LOCAL_RESULTS_DIR / f"{job_id}.stderr.log"
    stderr_path.write_text(stderr_tail, encoding="utf-8")
    record["stderr_tail_file"] = str(stderr_path)
    out_path = LOCAL_RESULTS_DIR / f"{job_id}.json"
    out_path.write_text(json.dumps(record, indent=2), encoding="utf-8")
    return out_path


def wait_for_local_result_file(job_id: str) -> Path:
    result_path = LOCAL_RESULTS_DIR / f"{job_id}.json"
    poll_seconds = DEFAULT_RESULTS_POLL_INTERVAL_SECONDS
    last_heartbeat = time.monotonic()
    while True:
        if result_path.exists():
            print(f"local_result_file: {result_path}")
            return result_path

        now = time.monotonic()
        if now - last_heartbeat >= 30:
            print(f"waiting for job {job_id} ...")
            last_heartbeat = now
        time.sleep(poll_seconds)


def _tail_text(path: Path, lines: int = 20) -> str:
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8", errors="replace")
    chunks = text.splitlines()
    return "\n".join(chunks[-lines:])


def ensure_local_watcher_running() -> None:
    require_env("CF_QUEUES_API_TOKEN")
    if LOCAL_WATCHER_PID_FILE.exists():
        pid = LOCAL_WATCHER_PID_FILE.read_text(encoding="utf-8").strip()
        if process_matches(pid, "local_pull_results.py --loop"):
            return

    LOCAL_WATCHER_PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    LOCAL_WATCHER_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.setdefault("PYTHON_BIN", sys.executable)
    env.setdefault("PYTHONUNBUFFERED", "1")
    with LOCAL_WATCHER_LOG_FILE.open("a", encoding="utf-8") as log_fp:
        proc = subprocess.Popen(
            [sys.executable, "-u", str(ROOT / "local-consumer" / "local_pull_results.py"), "--loop"],
            cwd=str(ROOT),
            stdout=log_fp,
            stderr=subprocess.STDOUT,
            env=env,
        )
    LOCAL_WATCHER_PID_FILE.write_text(str(proc.pid), encoding="utf-8")
    time.sleep(0.8)
    if not process_matches(str(proc.pid), "local_pull_results.py --loop"):
        tail = _tail_text(LOCAL_WATCHER_LOG_FILE, lines=30)
        raise RuntimeError(
            "local result watcher failed to start. "
            f"check {LOCAL_WATCHER_LOG_FILE}.\n{tail}"
        )


def submit_payload(payload: dict[str, Any], wait: bool) -> str:
    api_key = require_env("API_KEY")
    require_env("CF_QUEUES_API_TOKEN")
    ensure_local_watcher_running()
    worker_url = DEFAULT_WORKER_URL.rstrip("/")

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
        job_id = str(body.get("job_id", "")).strip()
        if not job_id:
            raise RuntimeError("submit succeeded but missing job_id")
        if not wait:
            LOCAL_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
            json_path = LOCAL_RESULTS_DIR / f"{job_id}.json"
            stdout_path = LOCAL_RESULTS_DIR / f"{job_id}.stdout.log"
            stderr_path = LOCAL_RESULTS_DIR / f"{job_id}.stderr.log"
            print(f"job queued: {job_id}")
            print(f"local_results_dir: {LOCAL_RESULTS_DIR.resolve()}")
            print(f"result_json: {json_path}")
            print(f"result_stdout: {stdout_path}")
            print(f"result_stderr: {stderr_path}")
            return job_id
        wait_for_local_result_file(job_id=job_id)
        return job_id
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"submit failed: HTTP {exc.code}: {detail}") from exc


def cmd_submit(raw_parts: list[str], wait: bool, exec_mode: str = "container") -> None:
    if not wait and "--wait" in raw_parts:
        wait = True
        raw_parts = [part for part in raw_parts if part != "--wait"]
    submit_input, rewritten = build_submit_input(raw_parts, exec_mode=exec_mode)
    if rewritten:
        print("warning: replaced python3.11 with python for runtime compatibility")
    payload = {"input": submit_input}
    job_id = submit_payload(payload=payload, wait=wait)
    if wait and job_id:
        cmd_logs(job_id)


def cmd_run_file(file_path: str, file_args: list[str], wait: bool, runner: str) -> None:
    if not wait and "--wait" in file_args:
        wait = True
        file_args = [part for part in file_args if part != "--wait"]
    payload = {
        "input": build_run_file_input(
            file_path=file_path,
            file_args=file_args,
            exec_mode="container",
            runner=runner,
        )
    }
    job_id = submit_payload(payload=payload, wait=wait)
    if wait and job_id:
        cmd_logs(job_id)


def maybe_refresh_image() -> None:
    print("refreshing Apptainer image...")
    try:
        run([str(ROOT / "hpc-consumer" / "scripts" / "update_apptainer_image.sh")], cwd=ROOT)
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            "image refresh failed. verify GitHub release SIF URLs are reachable."
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


def clear_single_queue(
    queue_label: str,
    queue_id: str,
    account_id: str,
    token: str,
    batch_size: int,
    max_batches: int,
) -> int:
    base = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/queues/{queue_id}/messages"
    total_acked = 0
    for _ in range(max_batches):
        pulled = cf_post(
            url=f"{base}/pull",
            token=token,
            payload={
                "batch_size": batch_size,
                "visibility_timeout_ms": 120000,
            },
        )
        messages = parse_messages(pulled)
        if not messages:
            break
        acks = [{"lease_id": m.get("lease_id")} for m in messages if m.get("lease_id")]
        if acks:
            cf_post(
                url=f"{base}/ack",
                token=token,
                payload={"acks": acks, "retries": []},
            )
            total_acked += len(acks)
    print(json.dumps({"queue": queue_label, "cleared_messages": total_acked}))
    return total_acked


def cmd_clear(target: str, batch_size: int, max_batches: int) -> None:
    token = require_env("CF_QUEUES_API_TOKEN")
    account_id = DEFAULT_CF_ACCOUNT_ID
    jobs_queue_id = DEFAULT_CF_JOBS_QUEUE_ID
    results_queue_id = DEFAULT_CF_RESULTS_QUEUE_ID

    total = 0
    if target in {"jobs", "all"}:
        total += clear_single_queue(
            queue_label="jobs",
            queue_id=jobs_queue_id,
            account_id=account_id,
            token=token,
            batch_size=batch_size,
            max_batches=max_batches,
        )
    if target in {"results", "all"}:
        total += clear_single_queue(
            queue_label="results",
            queue_id=results_queue_id,
            account_id=account_id,
            token=token,
            batch_size=batch_size,
            max_batches=max_batches,
        )
    print(json.dumps({"target": target, "total_cleared_messages": total}))


def cmd_logs(job_id: str) -> None:
    job_dir = ROOT / "results" / job_id
    if not job_dir.exists():
        # Backward compatibility with older layout.
        legacy_dir = ROOT / "hpc-consumer" / "results" / job_id
        if legacy_dir.exists():
            job_dir = legacy_dir
    meta_path = job_dir / "meta.json"
    stdout_path = job_dir / "stdout.log"
    stderr_path = job_dir / "stderr.log"
    local_result_json = LOCAL_RESULTS_DIR / f"{job_id}.json"
    local_stdout = LOCAL_RESULTS_DIR / f"{job_id}.stdout.log"
    local_stderr = LOCAL_RESULTS_DIR / f"{job_id}.stderr.log"

    if not job_dir.exists():
        if local_result_json.exists():
            record = json.loads(local_result_json.read_text(encoding="utf-8"))
            print(
                json.dumps(
                    {
                        "job_id": record.get("job_id"),
                        "event_type": record.get("event_type"),
                        "status": record.get("status"),
                        "exec_mode": record.get("exec_mode"),
                        "command": record.get("command"),
                        "workdir": record.get("workdir"),
                        "exit_code": record.get("exit_code"),
                        "started_at": record.get("started_at"),
                        "finished_at": record.get("finished_at"),
                        "result_pointer": record.get("result_pointer"),
                        "source": "local-results",
                    },
                    indent=2,
                )
            )
            print("\n=== stdout ===")
            if local_stdout.exists():
                print(local_stdout.read_text(encoding="utf-8"), end="")
            else:
                print("(missing)")
            print("\n=== stderr ===")
            if local_stderr.exists():
                print(local_stderr.read_text(encoding="utf-8"), end="")
            else:
                print("(missing)")
            print()
            return

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
                            "event_type": last_match.get("event_type"),
                            "status": last_match.get("status"),
                            "exec_mode": last_match.get("exec_mode"),
                            "command": last_match.get("command"),
                            "workdir": last_match.get("workdir"),
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


def cmd_job(job_id: str) -> None:
    local_result_json = LOCAL_RESULTS_DIR / f"{job_id}.json"
    if local_result_json.exists():
        record = json.loads(local_result_json.read_text(encoding="utf-8"))
        print(
            json.dumps(
                {
                    "job_id": record.get("job_id"),
                    "event_type": record.get("event_type"),
                    "status": record.get("status"),
                    "exec_mode": record.get("exec_mode"),
                    "command": record.get("command"),
                    "workdir": record.get("workdir"),
                    "exit_code": record.get("exit_code"),
                    "started_at": record.get("started_at"),
                    "finished_at": record.get("finished_at"),
                    "result_pointer": record.get("result_pointer"),
                    "source": "local-results",
                },
                indent=2,
            )
        )
        return

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
                        "event_type": last_match.get("event_type"),
                        "status": last_match.get("status"),
                        "exec_mode": last_match.get("exec_mode"),
                        "command": last_match.get("command"),
                        "workdir": last_match.get("workdir"),
                        "exit_code": last_match.get("exit_code"),
                        "started_at": last_match.get("started_at"),
                        "finished_at": last_match.get("finished_at"),
                        "result_pointer": last_match.get("result_pointer"),
                        "source": "results_cache",
                    },
                    indent=2,
                )
            )
            return

    print(json.dumps({"job_id": job_id, "status": "pending_or_unknown", "source": "local-cache"}, indent=2))


def cmd_status(output_json: bool = False) -> None:
    running = False
    pid = ""
    if PID_FILE.exists():
        pid = PID_FILE.read_text(encoding="utf-8").strip()
        running = process_matches(pid, "hpc_supervisor.py")
        if not running:
            pid = ""
    worker_pid = ""
    worker_running = False
    if WORKER_PID_FILE.exists():
        worker_pid = WORKER_PID_FILE.read_text(encoding="utf-8").strip()
        worker_running = process_matches(worker_pid, "hpc_pull_consumer.py")
        if not worker_running:
            worker_pid = ""

    local_running = False
    local_pid = ""
    if LOCAL_WATCHER_PID_FILE.exists():
        local_pid = LOCAL_WATCHER_PID_FILE.read_text(encoding="utf-8").strip()
        local_running = process_matches(local_pid, "local_pull_results.py --loop")
        if not local_running:
            local_pid = ""

    heartbeat = None
    heartbeat_age_seconds = None
    hpc_running_remote = None
    if HPC_STATUS_PATH.exists():
        try:
            heartbeat = json.loads(HPC_STATUS_PATH.read_text(encoding="utf-8"))
            ts_raw = str(heartbeat.get("timestamp", "")).strip()
            if ts_raw:
                ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                heartbeat_age_seconds = max(
                    0.0,
                    (datetime.now(timezone.utc) - ts.astimezone(timezone.utc)).total_seconds(),
                )
                max_age = DEFAULT_HPC_HEARTBEAT_MAX_AGE_SECONDS
                hpc_running_remote = heartbeat_age_seconds <= max_age
        except Exception:
            heartbeat = None

    # If this machine is running the HPC consumer, treat remote as healthy here.
    if hpc_running_remote is None and running:
        hpc_running_remote = True
        if heartbeat is None:
            heartbeat = {
                "event_type": "heartbeat",
                "status": "alive",
                "source": "local-process",
                "hostname": os.uname().nodename,
                "pid": int(pid) if pid.isdigit() else pid,
            }

    payload = {
        "running": running,
        "pid": pid or None,
        "worker_running": worker_running,
        "worker_pid": worker_pid or None,
        "local_results_watcher_running": local_running,
        "local_results_watcher_pid": local_pid or None,
        "hpc_running_remote": hpc_running_remote,
        "hpc_last_heartbeat": heartbeat,
        "hpc_heartbeat_age_seconds": heartbeat_age_seconds,
    }
    if output_json:
        print(json.dumps(payload))
        return

    this_host = os.uname().nodename
    hb_host = str((heartbeat or {}).get("hostname", "unknown"))
    print(f"host: {this_host}")
    client_mode = (
        not running
        and hb_host not in {"", "unknown"}
        and hb_host != this_host
    )
    if running:
        if worker_running:
            print(f"worker daemon (this machine): running (supervisor pid {pid}, worker pid {worker_pid})")
        else:
            print(f"worker daemon (this machine): restarting worker (supervisor pid {pid})")
    else:
        if client_mode:
            print("worker daemon (this machine): not running (client machine)")
        else:
            print("worker daemon (this machine): not running")

    if local_running:
        print(f"local results watcher: running (pid {local_pid})")
    else:
        print("local results watcher: not running")

    if hpc_running_remote is True:
        hb_pid = str((heartbeat or {}).get("pid", "unknown"))
        age = int(heartbeat_age_seconds or 0)
        print(f"remote worker heartbeat: healthy ({age}s ago, host={hb_host}, pid={hb_pid})")
    elif hpc_running_remote is False:
        age = int(heartbeat_age_seconds or 0)
        print(f"remote worker heartbeat: stale ({age}s ago, last_host={hb_host})")
    else:
        print("remote worker heartbeat: unknown (no heartbeat received yet)")


def cmd_stop(stop_all: bool) -> None:
    if PID_FILE.exists():
        pid = PID_FILE.read_text(encoding="utf-8").strip()
        if process_matches(pid, "hpc_supervisor.py"):
            subprocess.run(["kill", pid], check=False)
    if WORKER_PID_FILE.exists():
        worker_pid = WORKER_PID_FILE.read_text(encoding="utf-8").strip()
        if process_matches(worker_pid, "hpc_pull_consumer.py"):
            subprocess.run(["kill", worker_pid], check=False)
    if LOCAL_WATCHER_PID_FILE.exists():
        local_pid = LOCAL_WATCHER_PID_FILE.read_text(encoding="utf-8").strip()
        if process_matches(local_pid, "local_pull_results.py --loop"):
            subprocess.run(["kill", local_pid], check=False)
    if stop_all:
        cmd_clear(target="all", batch_size=100, max_batches=200)
    print("stop signal sent")


def get_latest_main_commit() -> str:
    proc = subprocess.run(
        ["git", "ls-remote", REPO_URL, "refs/heads/main"],
        check=True,
        capture_output=True,
        text=True,
    )
    line = (proc.stdout or "").strip().splitlines()
    if not line:
        raise RuntimeError("unable to resolve latest main commit")
    sha = line[0].split()[0].strip()
    if len(sha) != 40 or any(c not in "0123456789abcdef" for c in sha.lower()):
        raise RuntimeError(f"invalid commit hash resolved: {sha}")
    return sha


def cmd_update(wait: bool) -> None:
    latest = get_latest_main_commit()
    install_url = f"https://raw.githubusercontent.com/SauersML/hpc_queue/{latest}/install.sh"
    print(f"latest_main_commit: {latest}")
    print("updating local install...")
    run(["bash", "-lc", f"curl -fsSL {install_url} | bash"], cwd=ROOT)

    print("scheduling hpc update + graceful reload...")
    remote_script = (
        "set -euo pipefail; "
        f"curl -fsSL {install_url} | bash; "
        "mkdir -p \"$HOME/.local/share/hpc_queue/hpc-consumer\"; "
        "touch \"$HOME/.local/share/hpc_queue/hpc-consumer/reload_requested\"; "
        "echo hpc_update_requested"
    )
    payload = {
        "input": {
            "command": shlex.join(["bash", "-lc", remote_script]),
            "exec_mode": "host",
        }
    }
    job_id = submit_payload(payload=payload, wait=wait)
    print(f"hpc_update_job_id: {job_id}")
    print("note: hpc worker will drain in-flight jobs, then restart with new code")


def _read_local_result(job_id: str) -> dict[str, Any]:
    path = LOCAL_RESULTS_DIR / f"{job_id}.json"
    if not path.exists():
        raise RuntimeError(f"missing local result file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _best_output_path(default_name: str, output: str | None) -> Path:
    if output:
        candidate = Path(output).expanduser()
        if candidate.exists() and candidate.is_dir():
            return candidate / default_name
        if str(output).endswith("/"):
            return candidate / default_name
        return candidate
    return Path.cwd() / default_name


def cmd_grab(target: str, source_path: str, output: str | None) -> None:
    require_env("CF_QUEUES_API_TOKEN")
    api_key = require_env("API_KEY")
    src = source_path.strip()
    if not src:
        raise RuntimeError("grab requires a non-empty source path")

    basename = Path(src).name or "grab.bin"
    object_key = f"grab/{int(time.time())}-{secrets.token_hex(8)}-{basename}"
    worker_url = DEFAULT_WORKER_URL.rstrip("/")
    object_key_q = quote(object_key, safe="/._-~")
    upload_url = f"{worker_url}/grab/upload?object_key={object_key_q}"
    download_url = f"{worker_url}/grab/download?object_key={object_key_q}"
    delete_url = f"{worker_url}/grab/delete?object_key={object_key_q}"

    if target == "host":
        remote_script = (
            "set -euo pipefail; "
            f"SRC={shlex.quote(src)}; "
            "[ -f \"$SRC\" ] || { echo \"missing file: $SRC\" >&2; exit 1; }; "
            f"curl -fsS -X POST -H \"x-api-key: $API_KEY\" --data-binary @\"$SRC\" {shlex.quote(upload_url)} >/dev/null; "
            "echo uploaded"
        )
    else:
        job_id = target
        remote_script = (
            "set -euo pipefail; "
            f"JOB_ID={shlex.quote(job_id)}; "
            "ROOT=\"$HOME/.local/share/hpc_queue\"; "
            "JOB_DIR=\"$ROOT/results/$JOB_ID\"; "
            "[ -d \"$JOB_DIR\" ] || { echo \"missing job dir: $JOB_DIR\" >&2; exit 1; }; "
            f"TARGET={shlex.quote(src)}; "
            "IMG=\"$ROOT/runtime/hpc-queue-runtime.sif\"; "
            "APP=\"${APPTAINER_BIN:-apptainer}\"; "
            "[ -x \"$(command -v \"$APP\")\" ] || { echo \"apptainer not found\" >&2; exit 1; }; "
            f"$APP exec --bind \"$JOB_DIR:/work\" \"$IMG\" /bin/bash -lc "
            f"{shlex.quote('set -euo pipefail; [ -f \"$TARGET\" ] || { echo \"missing file in container: $TARGET\" >&2; exit 1; }; cat \"$TARGET\"')} "
            f"| curl -fsS -X POST -H \"x-api-key: $API_KEY\" --data-binary @- {shlex.quote(upload_url)} >/dev/null; "
            "echo uploaded"
        )

    payload = {"input": {"command": shlex.join(["bash", "-lc", remote_script]), "exec_mode": "host"}}
    job_id = submit_payload(payload=payload, wait=True)
    record = _read_local_result(job_id)
    if str(record.get("status")) != "completed" or int(record.get("exit_code", 1)) != 0:
        stderr_path = LOCAL_RESULTS_DIR / f"{job_id}.stderr.log"
        stderr_tail = stderr_path.read_text(encoding="utf-8", errors="replace") if stderr_path.exists() else ""
        raise RuntimeError(f"grab staging failed on hpc for job {job_id}\n{stderr_tail}")

    out_path = _best_output_path(default_name=basename, output=output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "curl",
            "-fsS",
            "-L",
            "-H",
            f"x-api-key: {api_key}",
            download_url,
            "-o",
            str(out_path),
        ],
        check=True,
    )
    data = out_path.read_bytes()
    print(f"saved: {out_path.resolve()}")
    print(f"bytes: {len(data)}")

    try:
        subprocess.run(
            ["curl", "-fsS", "-X", "POST", "-H", f"x-api-key: {api_key}", delete_url],
            check=True,
            capture_output=True,
        )
        print("remote_cleanup: deleted from private bucket")
    except Exception as exc:
        print(f"warning: remote_cleanup_failed: {exc}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="hpc_queue control CLI",
        epilog=(
            "Quickstart:\n"
            "  q login\n"
            "  q submit ls\n"
            "  q submit --wait \"python -V\"\n"
            "  q host --wait \"hostname\"\n"
            "  q logs <job_id>\n"
            "  q grab host /path/to/host/file -o ./\n"
            "  q status\n"
            "\n"
            "Container mounts:\n"
            "  /work    per-job scratch/results directory\n"
            "  /gnomon  synced gnomon repo on HPC\n"
            "  /reagle  synced reagle repo on HPC\n"
            "  /portal  read-only view of HPC host filesystem (/)\n"
            "\n"
            "Examples:\n"
            "  q submit --wait \"ls -lah /portal/users\"\n"
            "  q submit --wait \"ls -lah /portal/projects\"\n"
            "  q run-file --wait ./script.py -- --arg 1\n"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    submit = sub.add_parser(
        "submit",
        help="submit a shell command job (inside Apptainer)",
        description=(
            "Run command inside the container runtime on HPC.\n"
            "Inside container you can access /work, /gnomon, /reagle, and /portal (host filesystem, read-only)."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    submit.add_argument(
        "--wait",
        action="store_true",
        help="wait until local result file exists (default submits and returns immediately)",
    )
    submit.add_argument(
        "payload",
        nargs=argparse.REMAINDER,
        help="shell command to run inside container; use /portal/<host_path> for host files",
    )
    host = sub.add_parser(
        "host",
        help="submit a shell command job to run directly on HPC host",
        description="Run command directly on HPC host (outside container).",
    )
    host.add_argument(
        "--wait",
        action="store_true",
        help="wait until local result file exists (default submits and returns immediately)",
    )
    host.add_argument(
        "payload",
        nargs=argparse.REMAINDER,
        help="shell command to run on host (outside the container)",
    )
    run_file = sub.add_parser(
        "run-file",
        help="upload a local file and execute it inside container",
        description=(
            "Upload a local file into /work/files/<name> and execute it in container.\n"
            "Container can also read host files via /portal/<host_path>."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    run_file.add_argument(
        "--wait",
        action="store_true",
        help="wait until local result file exists (default submits and returns immediately)",
    )
    run_file.add_argument(
        "--runner",
        default="python",
        help='runner binary (default: "python"); use empty string to execute file directly',
    )
    run_file.add_argument("file_path", help="local file path to stage into the job")
    run_file.add_argument(
        "file_args",
        nargs=argparse.REMAINDER,
        help="arguments passed to the staged file",
    )

    login = sub.add_parser("login", help="configure local .env")
    login.add_argument("--queue-token", help="queue-token for Cloudflare Queue API")
    login.add_argument("--api-key", help="api-key for /jobs auth; auto-generated if omitted")
    sub.add_parser("start", help="start compute worker")
    sub.add_parser("worker", help="deprecated alias for start")
    sub.add_parser("results", help="pull one batch of results on local machine")
    clear_cmd = sub.add_parser("clear", help="clear messages from jobs/results queues")
    clear_cmd.add_argument("target", choices=["jobs", "results", "all"], help="which queue(s) to clear")
    clear_cmd.add_argument("--batch-size", type=int, default=100, help="messages per pull while clearing")
    clear_cmd.add_argument("--max-batches", type=int, default=200, help="maximum pull/ack cycles")
    logs = sub.add_parser("logs", help="show stdout/stderr for a completed job")
    logs.add_argument("job_id", help="job id to inspect from local results artifacts/cache")
    job = sub.add_parser("job", help="show last known status for one job from local cache")
    job.add_argument("job_id", help="job id to inspect")
    status_cmd = sub.add_parser("status", help="show worker status")
    status_cmd.add_argument("--json", action="store_true", help="output raw JSON status")
    stop_cmd = sub.add_parser("stop", help="stop worker process")
    stop_cmd.add_argument("--all", action="store_true", help="also clear jobs and results queues")
    update_cmd = sub.add_parser("update", help="update local+HPC to latest main commit")
    update_cmd.add_argument(
        "--no-wait",
        action="store_true",
        help="do not wait for the remote host update job result",
    )
    grab = sub.add_parser("grab", help="copy one file from HPC host/container to local via private R2")
    grab.add_argument("target", help='job id for container path mode, or literal "host" for host path mode')
    grab.add_argument("path", help="file path to grab")
    grab.add_argument("-o", "--output", help="local output file path (or target directory)")

    return parser


def normalize_wait_flag(argv: list[str]) -> list[str]:
    if not argv:
        return argv

    cmd = argv[0]
    if cmd not in {"submit", "host", "run-file"}:
        return argv
    if "--wait" not in argv[1:]:
        return argv

    rest = [part for part in argv[1:] if part != "--wait"]
    return [cmd, "--wait", *rest]


def main() -> None:
    load_dotenv(ENV_PATH)
    parser = build_parser()
    known_commands = {"submit", "host", "run-file", "login", "start", "worker", "results", "clear", "logs", "job", "status", "stop", "update", "grab"}
    argv = sys.argv[1:]
    if argv and argv[0] == "--update":
        argv = ["update", *argv[1:]]
    if argv and argv[0] not in known_commands and not argv[0].startswith("-"):
        # Shorthand: `q.py <command...>` behaves like `q.py submit <command...>`.
        argv = ["submit", *argv]
    argv = normalize_wait_flag(argv)
    args = parser.parse_args(argv)

    if args.command == "submit":
        cmd_submit(args.payload, args.wait, exec_mode="container")
    elif args.command == "host":
        cmd_submit(args.payload, args.wait, exec_mode="host")
    elif args.command == "run-file":
        cmd_run_file(args.file_path, args.file_args, args.wait, args.runner)
    elif args.command == "login":
        cmd_login(
            queue_token=args.queue_token,
            api_key=args.api_key,
        )
    elif args.command in {"start", "worker"}:
        cmd_worker()
    elif args.command == "results":
        cmd_results()
    elif args.command == "clear":
        cmd_clear(args.target, args.batch_size, args.max_batches)
    elif args.command == "logs":
        cmd_logs(args.job_id)
    elif args.command == "job":
        cmd_job(args.job_id)
    elif args.command == "status":
        cmd_status(args.json)
    elif args.command == "stop":
        cmd_stop(args.all)
    elif args.command == "update":
        cmd_update(wait=not args.no_wait)
    elif args.command == "grab":
        cmd_grab(target=args.target, source_path=args.path, output=args.output)
    else:
        parser.error("unknown command")


if __name__ == "__main__":
    main()
