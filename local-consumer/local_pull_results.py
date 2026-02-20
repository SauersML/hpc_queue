#!/usr/bin/env python3
"""Local pull reader for hpc-results queue.

Pulls one batch of result messages and acknowledges them after printing.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib import request

DEFAULT_CF_ACCOUNT_ID = "59908b351c3a3321ff84dd2d78bf0b42"
DEFAULT_CF_RESULTS_QUEUE_ID = "a435ae20f7514ce4b193879704b03e4e"
DEFAULT_RESULTS_BATCH_SIZE = 10
DEFAULT_RESULTS_VISIBILITY_TIMEOUT_MS = 120000
DEFAULT_RESULTS_POLL_INTERVAL_SECONDS = 2.0
DEFAULT_IDLE_EXIT_SECONDS = 600.0
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
RESULTS_CACHE_PATH = Path(__file__).resolve().parent.parent / "local-consumer" / "results_cache.jsonl"
HPC_STATUS_PATH = Path(__file__).resolve().parent.parent / "local-consumer" / "hpc_status.json"
LOCAL_RESULTS_DIR = Path(__file__).resolve().parent.parent / "local-results"


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


@dataclass
class Config:
    account_id: str
    results_queue_id: str
    api_token: str
    batch_size: int = 10
    visibility_timeout_ms: int = 120000
    poll_interval_seconds: float = 2.0

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
        results_queue_id=DEFAULT_CF_RESULTS_QUEUE_ID,
        api_token=req("CF_QUEUES_API_TOKEN"),
        batch_size=DEFAULT_RESULTS_BATCH_SIZE,
        visibility_timeout_ms=DEFAULT_RESULTS_VISIBILITY_TIMEOUT_MS,
        poll_interval_seconds=DEFAULT_RESULTS_POLL_INTERVAL_SECONDS,
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


def parse_messages(resp: dict[str, Any]) -> list[dict[str, Any]]:
    result = resp.get("result", {})
    if isinstance(result, dict):
        return result.get("messages", [])
    if isinstance(result, list):
        return result
    return []


def decode_body(body: Any, content_type: str) -> Any:
    if not isinstance(body, str):
        return body
    ct = content_type.strip().lower()
    if ct in {"json", "bytes"}:
        try:
            decoded = base64.b64decode(body)
        except Exception:
            return body
        if ct == "json":
            try:
                return json.loads(decoded.decode("utf-8"))
            except Exception:
                return body
        return decoded
    if ct == "text":
        try:
            return json.loads(body)
        except Exception:
            return body
    try:
        # Fallback: try json base64 first, then plain json.
        decoded = base64.b64decode(body)
        return json.loads(decoded.decode("utf-8"))
    except Exception:
        try:
            return json.loads(body)
        except Exception:
            return body


def process_once(config: Config) -> bool:
    pulled = cf_post(
        url=f"{config.results_api_base}/pull",
        token=config.api_token,
        payload={
            "batch_size": config.batch_size,
            "visibility_timeout_ms": config.visibility_timeout_ms,
        },
    )

    messages = parse_messages(pulled)
    if not messages:
        return False

    acks: list[dict[str, str]] = []
    RESULTS_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOCAL_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    for message in messages:
        lease_id = message.get("lease_id")
        if not lease_id:
            continue

        body = decode_body(message.get("body"), str(message.get("content_type", "")))

        cache_body = body
        if isinstance(body, (bytes, bytearray)):
            cache_body = {
                "_raw_bytes_b64": base64.b64encode(bytes(body)).decode("ascii"),
                "content_type": str(message.get("content_type", "")),
            }
        body_json = json.dumps(cache_body, separators=(",", ":"))
        print(body_json)
        with RESULTS_CACHE_PATH.open("a", encoding="utf-8") as cache_fp:
            cache_fp.write(body_json + "\n")
        if isinstance(body, dict):
            event_type = str(body.get("event_type", "")).strip()
            if event_type == "heartbeat":
                HPC_STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
                HPC_STATUS_PATH.write_text(json.dumps(body, indent=2), encoding="utf-8")
            job_id = str(body.get("job_id", "")).strip()
            status = str(body.get("status", "")).strip()
            if job_id and status in {"completed", "failed"}:
                record = dict(body)
                stdout_tail = str(record.pop("stdout_tail", ""))
                stderr_tail = str(record.pop("stderr_tail", ""))
                stdout_path = LOCAL_RESULTS_DIR / f"{job_id}.stdout.log"
                stdout_path.write_text(stdout_tail, encoding="utf-8")
                record["stdout_tail_file"] = str(stdout_path)
                stderr_path = LOCAL_RESULTS_DIR / f"{job_id}.stderr.log"
                stderr_path.write_text(stderr_tail, encoding="utf-8")
                record["stderr_tail_file"] = str(stderr_path)
                (LOCAL_RESULTS_DIR / f"{job_id}.json").write_text(
                    json.dumps(record, indent=2),
                    encoding="utf-8",
                )
        acks.append({"lease_id": lease_id})

    if acks:
        cf_post(
            url=f"{config.results_api_base}/ack",
            token=config.api_token,
            payload={"acks": acks, "retries": []},
        )
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Pull local result events from queue")
    parser.add_argument("--loop", action="store_true", help="run continuously in background")
    parser.add_argument(
        "--idle-exit-seconds",
        type=float,
        default=DEFAULT_IDLE_EXIT_SECONDS,
        help="exit loop mode after this many idle seconds with no messages (0 disables)",
    )
    args = parser.parse_args()

    load_dotenv(ENV_PATH)
    config = load_config()
    if not args.loop:
        try:
            process_once(config)
        except Exception as exc:
            print(f"results pull error: {exc}")
            raise
        return

    idle_exit_seconds = max(0.0, float(args.idle_exit_seconds))
    last_activity = time.monotonic()
    while True:
        try:
            had_messages = process_once(config)
            if had_messages:
                last_activity = time.monotonic()
            elif idle_exit_seconds > 0 and (time.monotonic() - last_activity) >= idle_exit_seconds:
                print(f"results watcher idle for {idle_exit_seconds:.0f}s; exiting")
                return
        except Exception as exc:
            print(f"results loop error: {exc}")
        time.sleep(config.poll_interval_seconds)


if __name__ == "__main__":
    main()
