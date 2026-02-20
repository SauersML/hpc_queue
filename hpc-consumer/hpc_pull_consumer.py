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
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import request

DEFAULT_CF_ACCOUNT_ID = "59908b351c3a3321ff84dd2d78bf0b42"
DEFAULT_CF_JOBS_QUEUE_ID = "f52e2e6bb569425894ede9141e9343a5"
DEFAULT_CF_RESULTS_QUEUE_ID = "a435ae20f7514ce4b193879704b03e4e"
DEFAULT_PULL_BATCH_SIZE = 100
DEFAULT_VISIBILITY_TIMEOUT_MS = 120000
DEFAULT_POLL_INTERVAL_SECONDS = 2.0
DEFAULT_MAX_IDLE_POLL_SECONDS = 30.0
DEFAULT_RETRY_DELAY_SECONDS = 30
DEFAULT_MAX_RETRY_ATTEMPTS = 5
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 600.0
ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_RESULTS_DIR = "results"
DEFAULT_APPTAINER_IMAGE = str(ROOT_DIR / "runtime" / "hpc-queue-runtime.sif")
DEFAULT_APPTAINER_BIN = "apptainer"
DEFAULT_APPTAINER_BIND = ""
DEFAULT_CONTAINER_CMD = "python /app/run.py"
DEFAULT_HOST_PORTAL_BIND = "/:/portal:ro"
DEFAULT_EXTERNAL_REPOS_ROOT = str(ROOT_DIR / "runtime" / "external-src")
DEFAULT_GNOMON_REPO_URL = "https://github.com/SauersML/gnomon.git"
DEFAULT_REAGLE_REPO_URL = "https://github.com/SauersML/reagle.git"
DEFAULT_REPO_REF = "main"
RELOAD_REQUEST_PATH = ROOT_DIR / "hpc-consumer" / "reload_requested"


@dataclass
class Config:
    account_id: str
    jobs_queue_id: str
    results_queue_id: str
    api_token: str
    visibility_timeout_ms: int = 120000
    poll_interval_seconds: float = 2.0
    retry_delay_seconds: int = 30
    max_retry_attempts: int = 5
    heartbeat_interval_seconds: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS
    results_dir: str = "results"
    apptainer_image: str = ""
    apptainer_bin: str = "apptainer"
    apptainer_bind: str = ""
    container_cmd: str = "python /app/run.py"
    external_repos_root: str = DEFAULT_EXTERNAL_REPOS_ROOT
    gnomon_repo_url: str = DEFAULT_GNOMON_REPO_URL
    gnomon_repo_ref: str = DEFAULT_REPO_REF
    reagle_repo_url: str = DEFAULT_REAGLE_REPO_URL
    reagle_repo_ref: str = DEFAULT_REPO_REF

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
        account_id=DEFAULT_CF_ACCOUNT_ID,
        jobs_queue_id=DEFAULT_CF_JOBS_QUEUE_ID,
        results_queue_id=DEFAULT_CF_RESULTS_QUEUE_ID,
        api_token=req("CF_QUEUES_API_TOKEN"),
        visibility_timeout_ms=DEFAULT_VISIBILITY_TIMEOUT_MS,
        poll_interval_seconds=DEFAULT_POLL_INTERVAL_SECONDS,
        retry_delay_seconds=DEFAULT_RETRY_DELAY_SECONDS,
        max_retry_attempts=DEFAULT_MAX_RETRY_ATTEMPTS,
        heartbeat_interval_seconds=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        results_dir=DEFAULT_RESULTS_DIR,
        apptainer_image=DEFAULT_APPTAINER_IMAGE,
        apptainer_bin=DEFAULT_APPTAINER_BIN,
        apptainer_bind=DEFAULT_APPTAINER_BIND,
        container_cmd=DEFAULT_CONTAINER_CMD,
        external_repos_root=DEFAULT_EXTERNAL_REPOS_ROOT,
        gnomon_repo_url=DEFAULT_GNOMON_REPO_URL,
        gnomon_repo_ref=DEFAULT_REPO_REF,
        reagle_repo_url=DEFAULT_REAGLE_REPO_URL,
        reagle_repo_ref=DEFAULT_REPO_REF,
    )


def _git(*args: str, cwd: Path | None = None) -> str:
    proc = subprocess.run(
        ["git", *args],
        cwd=str(cwd) if cwd else None,
        check=True,
        capture_output=True,
        text=True,
    )
    return (proc.stdout or "").strip()


def sync_external_repo(name: str, url: str, ref: str, repos_root: Path) -> dict[str, str]:
    repo_dir = repos_root / name
    repo_dir.parent.mkdir(parents=True, exist_ok=True)

    if (repo_dir / ".git").exists():
        _git("-C", str(repo_dir), "remote", "set-url", "origin", url)
        _git("-C", str(repo_dir), "fetch", "--depth", "1", "origin", ref)
        _git("-C", str(repo_dir), "reset", "--hard", "FETCH_HEAD")
        _git("-C", str(repo_dir), "clean", "-fdx")
    else:
        _git("clone", "--depth", "1", "--branch", ref, url, str(repo_dir))

    sha = _git("-C", str(repo_dir), "rev-parse", "HEAD")
    return {"name": name, "path": str(repo_dir.resolve()), "ref": ref, "sha": sha}


def sync_external_repos(config: Config) -> list[dict[str, str]]:
    repos_root = Path(config.external_repos_root)
    repos = [
        ("gnomon", config.gnomon_repo_url, config.gnomon_repo_ref),
        ("reagle", config.reagle_repo_url, config.reagle_repo_ref),
    ]
    synced: list[dict[str, str]] = []
    for name, url, ref in repos:
        synced.append(sync_external_repo(name, url, ref, repos_root))
    return synced


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
    return decode_message_body_with_content_type(body=body, content_type="")


def decode_message_body_with_content_type(body: Any, content_type: str) -> dict[str, Any]:
    if isinstance(body, dict):
        return body

    if isinstance(body, str):
        ct = content_type.strip().lower()
        # For json content, body may be base64-encoded in pull responses.
        if ct in {"json", ""}:
            try:
                decoded = base64.b64decode(body)
                return json.loads(decoded.decode("utf-8"))
            except Exception:
                try:
                    return json.loads(body)
                except Exception as exc:
                    raise ValueError(f"Unable to decode JSON message body: {body}") from exc
        if ct == "text":
            try:
                return json.loads(body)
            except Exception as exc:
                raise ValueError(f"Unsupported text message body; expected JSON object: {body}") from exc
        if ct == "bytes":
            raise ValueError("Unsupported bytes message body for jobs queue; expected JSON content type")

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
    apptainer_stdout_path = job_dir / "apptainer.stdout.log"
    apptainer_stderr_path = job_dir / "apptainer.stderr.log"

    job_input = job.get("input", {})
    if not isinstance(job_input, dict):
        job_input = {}
    command = str(job_input.get("command", ""))
    input_path.write_text(json.dumps({"job_id": job_id, "input": job_input}), encoding="utf-8")
    staged_files = stage_local_files(job_input, job_dir)
    try:
        synced_repos = sync_external_repos(config)
    except Exception as exc:
        raise RuntimeError(f"external repo sync failed: {exc}") from exc

    cmd = [
        config.apptainer_bin,
        "exec",
        "--bind",
        f"{job_dir}:/work",
        "--bind",
        DEFAULT_HOST_PORTAL_BIND,
    ]
    for repo in synced_repos:
        cmd.extend(["--bind", f"{repo['path']}:/{repo['name']}"])
    if config.apptainer_bind:
        cmd.extend(["--bind", config.apptainer_bind])
    cmd.extend([config.apptainer_image, "/bin/bash", "-lc", config.container_cmd])

    started = datetime.now(timezone.utc).isoformat()
    with apptainer_stdout_path.open("w", encoding="utf-8") as stdout_fp, apptainer_stderr_path.open(
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
        "command": command,
        "workdir": "/",
        "status": "completed" if proc.returncode == 0 else "failed",
        "started_at": started,
        "finished_at": finished,
        "returncode": proc.returncode,
        "staged_files": staged_files,
        "synced_repos": synced_repos,
        "stdout_tail": (tail_text(apptainer_stdout_path) + tail_text(stdout_path))[-8000:],
        "stderr_tail": (tail_text(apptainer_stderr_path) + tail_text(stderr_path))[-8000:],
        "stdout_path": str(stdout_path.resolve()),
        "stderr_path": str(stderr_path.resolve()),
        "apptainer_stdout_path": str(apptainer_stdout_path.resolve()),
        "apptainer_stderr_path": str(apptainer_stderr_path.resolve()),
        "input_path": str(input_path.resolve()),
        "output_path": str(output_path.resolve()),
    }
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

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
            cwd=str(job_dir.resolve()),
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
        "workdir": str(job_dir.resolve()),
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


IMAGE_REFRESH_LOCK = threading.Lock()


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


def heartbeat_loop(config: Config) -> None:
    interval = max(1.0, config.heartbeat_interval_seconds)
    while True:
        try:
            enqueue_heartbeat(config)
        except Exception as exc:
            print(f"heartbeat error: {exc}")
        time.sleep(interval)


def process_message(message: dict[str, Any], config: Config, results_dir: Path) -> dict[str, Any] | None:
    lease_id = message.get("lease_id")
    if not lease_id:
        return None

    job_id = "unknown"
    try:
        job = decode_message_body_with_content_type(
            body=message.get("body"),
            content_type=str(message.get("content_type", "")),
        )
        job_id = str(job.get("job_id", "unknown"))
        job_input = job.get("input", {})
        exec_mode = (
            str(job_input.get("exec_mode", "container")).lower()
            if isinstance(job_input, dict)
            else "container"
        )
        if exec_mode == "host":
            result_pointer, exit_code, meta = run_host_compute(job, results_dir)
        else:
            with IMAGE_REFRESH_LOCK:
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
                "command": meta.get("command", ""),
                "workdir": meta.get("workdir", ""),
                "exit_code": exit_code,
                "stdout_tail": meta.get("stdout_tail", ""),
                "stderr_tail": meta.get("stderr_tail", ""),
                "started_at": meta.get("started_at"),
                "finished_at": meta.get("finished_at"),
            },
        )
        print(f"completed job {job_id} -> {result_pointer}")
        return {"action": "ack", "lease_id": str(lease_id)}
    except Exception as exc:
        err = str(exc)
        print(f"failed to process message job_id={job_id}: {err}")
        attempts_raw = message.get("attempts", 0)
        try:
            attempts = int(attempts_raw)
        except Exception:
            attempts = 0
        if attempts < config.max_retry_attempts:
            print(
                "scheduling retry "
                f"job_id={job_id} attempts={attempts}/{config.max_retry_attempts} "
                f"delay={config.retry_delay_seconds}s"
            )
            return {
                "action": "retry",
                "lease_id": str(lease_id),
                "delay_seconds": config.retry_delay_seconds,
            }

        now = datetime.now(timezone.utc).isoformat()
        try:
            enqueue_result(
                config=config,
                job_id=job_id,
                status="failed",
                result_pointer="",
                extra={
                    "event_type": "failed",
                    "exit_code": 1,
                    "stderr_tail": err[-8000:],
                    "started_at": now,
                    "finished_at": now,
                    "attempts": attempts,
                },
            )
        except Exception as enqueue_exc:
            print(f"failed to enqueue failure event for job_id={job_id}: {enqueue_exc}")
        return {"action": "ack", "lease_id": str(lease_id)}

def ack_retry_outcomes(config: Config, outcomes: list[dict[str, Any]]) -> None:
    acks: list[dict[str, str]] = []
    retries: list[dict[str, Any]] = []
    for outcome in outcomes:
        if not outcome:
            continue
        action = outcome.get("action")
        lease_id = str(outcome.get("lease_id", "")).strip()
        if not lease_id:
            continue
        if action == "retry":
            retries.append(
                {
                    "lease_id": lease_id,
                    "delay_seconds": int(outcome.get("delay_seconds", config.retry_delay_seconds)),
                }
            )
        else:
            acks.append({"lease_id": lease_id})
    if not acks and not retries:
        return
    cf_post(
        url=f"{config.jobs_api_base}/ack",
        token=config.api_token,
        payload={"acks": acks, "retries": retries},
    )


def process_message_worker(
    message: dict[str, Any],
    config: Config,
    results_dir: Path,
    completed_outcomes: list[dict[str, Any]],
    completed_lock: threading.Lock,
) -> None:
    try:
        outcome = process_message(message, config, results_dir)
        if outcome:
            with completed_lock:
                completed_outcomes.append(outcome)
    except Exception as exc:
        # process_message should already handle exceptions; this is a hard safety net.
        print(f"job worker crashed unexpectedly: {exc}")


def process_once(
    config: Config,
    inflight: dict[threading.Thread, bool],
    results_dir: Path,
    completed_outcomes: list[dict[str, Any]],
    completed_lock: threading.Lock,
    allow_pull: bool = True,
) -> bool:
    had_activity = False
    done: list[threading.Thread] = [thread for thread in list(inflight.keys()) if not thread.is_alive()]
    if done:
        for thread in done:
            inflight.pop(thread, None)
            thread.join(timeout=0)

    with completed_lock:
        outcomes_to_ack = list(completed_outcomes)
        completed_outcomes.clear()
    if outcomes_to_ack:
        ack_retry_outcomes(config, outcomes_to_ack)
        had_activity = True

    if not allow_pull:
        return had_activity

    pull_resp = cf_post(
        url=f"{config.jobs_api_base}/pull",
        token=config.api_token,
        payload={
            "batch_size": DEFAULT_PULL_BATCH_SIZE,
            "visibility_timeout_ms": config.visibility_timeout_ms,
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
        return had_activity

    for message in messages:
        thread = threading.Thread(
            target=process_message_worker,
            args=(message, config, results_dir, completed_outcomes, completed_lock),
            daemon=True,
            name="job-worker",
        )
        inflight[thread] = True
        thread.start()
    return True


def main() -> None:
    config = load_config()
    results_dir = Path(config.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    print("starting hpc pull consumer")
    print(
        json.dumps(
            {
                "jobs_queue_id": config.jobs_queue_id,
                "results_queue_id": config.results_queue_id,
                "batch_size": DEFAULT_PULL_BATCH_SIZE,
                "visibility_timeout_ms": config.visibility_timeout_ms,
                "poll_interval_seconds": config.poll_interval_seconds,
            }
        )
    )

    heartbeat_thread = threading.Thread(
        target=heartbeat_loop,
        args=(config,),
        daemon=True,
        name="heartbeat-loop",
    )
    heartbeat_thread.start()

    inflight: dict[threading.Thread, bool] = {}
    completed_outcomes: list[dict[str, Any]] = []
    completed_lock = threading.Lock()
    drain_mode = False
    idle_streak = 0
    while True:
        try:
            if not drain_mode and RELOAD_REQUEST_PATH.exists():
                drain_mode = True
                print("reload requested; entering drain mode (no new pulls)")

            had_activity = process_once(
                config,
                inflight,
                results_dir,
                completed_outcomes,
                completed_lock,
                allow_pull=not drain_mode,
            )

            if drain_mode and not inflight:
                RELOAD_REQUEST_PATH.unlink(missing_ok=True)
                print("drain complete; exiting for supervisor restart")
                return
            if had_activity or inflight:
                idle_streak = 0
            else:
                idle_streak = min(idle_streak + 1, 8)
        except Exception as exc:
            print(f"poll loop error: {exc}")
        sleep_seconds = min(
            DEFAULT_MAX_IDLE_POLL_SECONDS,
            config.poll_interval_seconds * (2 ** idle_streak),
        )
        time.sleep(max(1.0, sleep_seconds))


if __name__ == "__main__":
    main()
