#!/usr/bin/env python3
"""HPC pull consumer for Cloudflare Queues.

This process runs on the compute node and repeatedly:
1) pulls jobs from Cloudflare Queues
2) runs compute work
3) acknowledges success or marks failed jobs for retry
"""

from __future__ import annotations

import base64
import json
import os
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import request

DEFAULT_CF_ACCOUNT_ID = "59908b351c3a3321ff84dd2d78bf0b42"
DEFAULT_CF_JOBS_QUEUE_ID = "f52e2e6bb569425894ede9141e9343a5"
DEFAULT_CF_RESULTS_QUEUE_ID = "a435ae20f7514ce4b193879704b03e4e"
ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_APPTAINER_IMAGE = str(ROOT_DIR / "runtime" / "hpc-queue-runtime.sif")


@dataclass
class Config:
    account_id: str
    jobs_queue_id: str
    results_queue_id: str
    api_token: str
    batch_size: int = 5
    visibility_timeout_ms: int = 120000
    poll_interval_seconds: float = 2.0
    retry_delay_seconds: int = 30
    heartbeat_interval_seconds: float = 30.0
    results_dir: str = "results"
    apptainer_image: str = ""
    apptainer_bin: str = "apptainer"
    apptainer_bind: str = ""
    container_cmd: str = "python /app/run.py"

    @property
    def jobs_api_base(self) -> str:
        return (
            "https://api.cloudflare.com/client/v4/accounts/"
            f"{self.account_id}/queues/{self.jobs_queue_id}/messages"
        )

    @property
    def results_api_base(self) -> str:
        return (
            "https://api.cloudflare.com/client/v4/accounts/"
            f"{self.account_id}/queues/{self.results_queue_id}/messages"
        )


def load_config() -> Config:
    def req(name: str) -> str:
        val = os.getenv(name)
        if not val:
            raise RuntimeError(f"Missing required env var: {name}")
        return val

    return Config(
        account_id=os.getenv("CF_ACCOUNT_ID", DEFAULT_CF_ACCOUNT_ID),
        jobs_queue_id=os.getenv("CF_JOBS_QUEUE_ID", DEFAULT_CF_JOBS_QUEUE_ID),
        results_queue_id=os.getenv("CF_RESULTS_QUEUE_ID", DEFAULT_CF_RESULTS_QUEUE_ID),
        api_token=req("CF_QUEUES_API_TOKEN"),
        batch_size=int(os.getenv("BATCH_SIZE", "5")),
        visibility_timeout_ms=int(os.getenv("VISIBILITY_TIMEOUT_MS", "120000")),
        poll_interval_seconds=float(os.getenv("POLL_INTERVAL_SECONDS", "2")),
        retry_delay_seconds=int(os.getenv("RETRY_DELAY_SECONDS", "30")),
        heartbeat_interval_seconds=float(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "30")),
        results_dir=os.getenv("RESULTS_DIR", "results"),
        apptainer_image=os.getenv("APPTAINER_IMAGE", DEFAULT_APPTAINER_IMAGE),
        apptainer_bin=os.getenv("APPTAINER_BIN", "apptainer"),
        apptainer_bind=os.getenv("APPTAINER_BIND", ""),
        container_cmd=os.getenv(
            "CONTAINER_CMD",
            "python /app/run.py",
        ),
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
        raw = resp.read().decode("utf-8")
    return json.loads(raw)


def decode_message_body(body: Any) -> dict[str, Any]:
    if isinstance(body, dict):
        return body

    if isinstance(body, str):
        # Pull consumers often receive json content base64-encoded.
        try:
            decoded = base64.b64decode(body)
            return json.loads(decoded.decode("utf-8"))
        except Exception:
            try:
                return json.loads(body)
            except Exception as exc:
                raise ValueError(f"Unable to decode message body: {body}") from exc

    raise ValueError(f"Unsupported body type: {type(body)}")


def stage_local_files(job_input: dict[str, Any], job_dir: Path) -> list[str]:
    local_files = job_input.get("local_files", [])
    if not isinstance(local_files, list):
        return []

    staged: list[str] = []
    for item in local_files:
        if not isinstance(item, dict):
            continue
        rel = str(item.get("path", "")).strip()
        data_b64 = str(item.get("content_b64", "")).strip()
        if not rel or not data_b64:
            continue

        rel_path = Path(rel)
        if rel_path.is_absolute() or ".." in rel_path.parts:
            raise ValueError(f"invalid local_files path: {rel}")

        try:
            data = base64.b64decode(data_b64)
        except Exception as exc:
            raise ValueError(f"invalid base64 for staged file path={rel}") from exc

        target = job_dir / rel_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)

        mode_raw = str(item.get("mode", "644")).strip()
        try:
            mode = int(mode_raw, 8)
        except Exception:
            mode = 0o644
        os.chmod(target, mode)
        staged.append(str(target.resolve()))
    return staged


def run_compute(job: dict[str, Any], results_dir: Path, config: Config) -> tuple[str, int, dict[str, Any]]:
    """Run compute directly on node using Apptainer."""
    job_id = str(job.get("job_id", "unknown"))
    job_dir = results_dir / job_id
    job_dir.mkdir(parents=True, exist_ok=True)

    input_path = job_dir / "input.json"
    output_path = job_dir / "output.json"
    meta_path = job_dir / "meta.json"
    stdout_path = job_dir / "stdout.log"
    stderr_path = job_dir / "stderr.log"

    job_input = job.get("input", {})
    if not isinstance(job_input, dict):
        job_input = {}
    input_path.write_text(json.dumps({"job_id": job_id, "input": job_input}), encoding="utf-8")
    staged_files = stage_local_files(job_input, job_dir)

    cmd = [
        config.apptainer_bin,
        "exec",
        "--bind",
        f"{job_dir}:/work",
    ]
    if config.apptainer_bind:
        cmd.extend(["--bind", config.apptainer_bind])
    cmd.extend([config.apptainer_image, "/bin/bash", "-lc", config.container_cmd])

    started = datetime.now(timezone.utc).isoformat()
    with stdout_path.open("w", encoding="utf-8") as stdout_fp, stderr_path.open(
        "w", encoding="utf-8"
    ) as stderr_fp:
        proc = subprocess.run(cmd, stdout=stdout_fp, stderr=stderr_fp, text=True)
    finished = datetime.now(timezone.utc).isoformat()

    def tail_text(path: Path, chars: int = 8000) -> str:
        try:
            return path.read_text(encoding="utf-8")[-chars:]
        except Exception:
            return ""

    meta = {
        "job_id": job_id,
        "exec_mode": "container",
        "status": "completed" if proc.returncode == 0 else "failed",
        "started_at": started,
        "finished_at": finished,
        "returncode": proc.returncode,
        "staged_files": staged_files,
        "stdout_tail": tail_text(stdout_path),
        "stderr_tail": tail_text(stderr_path),
        "stdout_path": str(stdout_path.resolve()),
        "stderr_path": str(stderr_path.resolve()),
        "input_path": str(input_path.resolve()),
        "output_path": str(output_path.resolve()),
    }
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    if proc.returncode != 0:
        raise RuntimeError(
            f"apptainer command failed for {job_id} rc={proc.returncode}: {meta['stderr_tail'][-500:]}"
        )

    if not output_path.exists():
        output_path.write_text(
            json.dumps(
                {
                    "job_id": job_id,
                    "finished_at": finished,
                    "status": "completed",
                    "result": {"note": "container exited 0 but no output.json produced"},
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    return str(output_path.resolve()), proc.returncode, meta


def run_host_compute(job: dict[str, Any], results_dir: Path) -> tuple[str, int, dict[str, Any]]:
    """Run compute directly on host node (outside Apptainer)."""
    job_id = str(job.get("job_id", "unknown"))
    job_dir = results_dir / job_id
    job_dir.mkdir(parents=True, exist_ok=True)

    input_path = job_dir / "input.json"
    output_path = job_dir / "output.json"
    meta_path = job_dir / "meta.json"
    stdout_path = job_dir / "stdout.log"
    stderr_path = job_dir / "stderr.log"

    job_input = job.get("input", {})
    if not isinstance(job_input, dict):
        job_input = {}
    input_path.write_text(json.dumps({"job_id": job_id, "input": job_input}), encoding="utf-8")
    staged_files = stage_local_files(job_input, job_dir)
    command = str(job_input.get("command", "echo no command provided"))

    started = datetime.now(timezone.utc).isoformat()
    with stdout_path.open("w", encoding="utf-8") as stdout_fp, stderr_path.open(
        "w", encoding="utf-8"
    ) as stderr_fp:
        proc = subprocess.run(
            ["/bin/bash", "-lc", command],
            stdout=stdout_fp,
            stderr=stderr_fp,
            text=True,
        )
    finished = datetime.now(timezone.utc).isoformat()

    def tail_text(path: Path, chars: int = 8000) -> str:
        try:
            return path.read_text(encoding="utf-8")[-chars:]
        except Exception:
            return ""

    meta = {
        "job_id": job_id,
        "exec_mode": "host",
        "command": command,
        "status": "completed" if proc.returncode == 0 else "failed",
        "started_at": started,
        "finished_at": finished,
        "returncode": proc.returncode,
        "staged_files": staged_files,
        "stdout_tail": tail_text(stdout_path),
        "stderr_tail": tail_text(stderr_path),
        "stdout_path": str(stdout_path.resolve()),
        "stderr_path": str(stderr_path.resolve()),
        "input_path": str(input_path.resolve()),
        "output_path": str(output_path.resolve()),
    }
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    output_path.write_text(
        json.dumps(
            {
                "job_id": job_id,
                "exec_mode": "host",
                "command": command,
                "status": "completed" if proc.returncode == 0 else "failed",
                "started_at": started,
                "finished_at": finished,
                "exit_code": proc.returncode,
                "result": {
                    "stdout_path": str(stdout_path.resolve()),
                    "stderr_path": str(stderr_path.resolve()),
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    return str(output_path.resolve()), proc.returncode, meta


def ensure_image_fresh() -> None:
    """Ensure local SIF matches current remote digest before running a job."""
    updater = ROOT_DIR / "hpc-consumer" / "scripts" / "update_apptainer_image.sh"
    subprocess.run([str(updater)], cwd=str(ROOT_DIR), check=True)


def enqueue_result(
    config: Config,
    job_id: str,
    status: str,
    result_pointer: str,
    extra: dict[str, Any] | None = None,
) -> None:
    payload = {
        "job_id": job_id,
        "status": status,
        "result_pointer": result_pointer,
    }
    if extra:
        payload.update(extra)
    cf_post(
        url=config.results_api_base,
        token=config.api_token,
        payload={"body": payload},
    )


def enqueue_heartbeat(config: Config) -> None:
    now = datetime.now(timezone.utc).isoformat()
    payload = {
        "event_type": "heartbeat",
        "status": "alive",
        "source": "hpc-consumer",
        "hostname": os.uname().nodename,
        "pid": os.getpid(),
        "timestamp": now,
    }
    cf_post(
        url=config.results_api_base,
        token=config.api_token,
        payload={"body": payload},
    )


def process_once(config: Config) -> None:
    pull_resp = cf_post(
        url=f"{config.jobs_api_base}/pull",
        token=config.api_token,
        payload={
            "batch_size": config.batch_size,
            "visibility_timeout": config.visibility_timeout_ms,
        },
    )

    result = pull_resp.get("result", {})
    if isinstance(result, dict):
        messages = result.get("messages", [])
    elif isinstance(result, list):
        messages = result
    else:
        messages = []
    if not messages:
        return

    acks: list[dict[str, str]] = []
    retries: list[dict[str, Any]] = []
    results_dir = Path(config.results_dir)

    for message in messages:
        lease_id = message.get("lease_id")
        if not lease_id:
            continue

        try:
            job = decode_message_body(message.get("body"))
            job_id = str(job.get("job_id", "unknown"))
            job_input = job.get("input", {})
            exec_mode = str(job_input.get("exec_mode", "container")).lower() if isinstance(job_input, dict) else "container"
            if exec_mode == "host":
                result_pointer, exit_code, meta = run_host_compute(job, results_dir)
            else:
                ensure_image_fresh()
                result_pointer, exit_code, meta = run_compute(job, results_dir, config)
            enqueue_result(
                config=config,
                job_id=job_id,
                status="completed" if exit_code == 0 else "failed",
                result_pointer=result_pointer,
                extra={
                    "event_type": "completed",
                    "exec_mode": exec_mode,
                    "exit_code": exit_code,
                    "stdout_tail": meta.get("stdout_tail", ""),
                    "stderr_tail": meta.get("stderr_tail", ""),
                    "started_at": meta.get("started_at"),
                    "finished_at": meta.get("finished_at"),
                },
            )
            acks.append({"lease_id": lease_id})
            print(f"completed job {job_id} -> {result_pointer}")
        except Exception as exc:
            print(f"failed to process message: {exc}")
            retries.append(
                {
                    "lease_id": lease_id,
                    "delay_seconds": config.retry_delay_seconds,
                }
            )

    if acks or retries:
        cf_post(
            url=f"{config.jobs_api_base}/ack",
            token=config.api_token,
            payload={"acks": acks, "retries": retries},
        )


def main() -> None:
    config = load_config()
    print("starting hpc pull consumer")
    print(
        json.dumps(
            {
                "jobs_queue_id": config.jobs_queue_id,
                "results_queue_id": config.results_queue_id,
                "batch_size": config.batch_size,
                "visibility_timeout_ms": config.visibility_timeout_ms,
                "poll_interval_seconds": config.poll_interval_seconds,
            }
        )
    )

    last_heartbeat = 0.0
    while True:
        try:
            now = time.monotonic()
            if now - last_heartbeat >= max(1.0, config.heartbeat_interval_seconds):
                enqueue_heartbeat(config)
                last_heartbeat = now
            process_once(config)
        except Exception as exc:
            print(f"poll loop error: {exc}")
        time.sleep(config.poll_interval_seconds)


if __name__ == "__main__":
    main()
