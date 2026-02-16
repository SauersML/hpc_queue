#!/usr/bin/env python3
"""Laptop pull consumer for hpc-results queue.

Pulls result messages and acknowledges them after printing.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any
from urllib import request

DEFAULT_CF_ACCOUNT_ID = "59908b351c3a3321ff84dd2d78bf0b42"
DEFAULT_CF_RESULTS_QUEUE_ID = "a435ae20f7514ce4b193879704b03e4e"


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
        account_id=os.getenv("CF_ACCOUNT_ID", DEFAULT_CF_ACCOUNT_ID),
        results_queue_id=os.getenv("CF_RESULTS_QUEUE_ID", DEFAULT_CF_RESULTS_QUEUE_ID),
        api_token=req("CF_QUEUES_API_TOKEN"),
        batch_size=int(os.getenv("RESULTS_BATCH_SIZE", "10")),
        visibility_timeout_ms=int(os.getenv("RESULTS_VISIBILITY_TIMEOUT_MS", "120000")),
        poll_interval_seconds=float(os.getenv("RESULTS_POLL_INTERVAL_SECONDS", "2")),
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


def process_once(config: Config) -> None:
    pulled = cf_post(
        url=f"{config.results_api_base}/pull",
        token=config.api_token,
        payload={
            "batch_size": config.batch_size,
            "visibility_timeout": config.visibility_timeout_ms,
        },
    )

    messages = parse_messages(pulled)
    if not messages:
        return

    acks: list[dict[str, str]] = []
    for message in messages:
        lease_id = message.get("lease_id")
        if not lease_id:
            continue

        body = message.get("body")
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except Exception:
                pass

        print(json.dumps(body, separators=(",", ":")))
        acks.append({"lease_id": lease_id})

    if acks:
        cf_post(
            url=f"{config.results_api_base}/ack",
            token=config.api_token,
            payload={"acks": acks, "retries": []},
        )


def main() -> None:
    config = load_config()
    print("starting laptop results pull consumer")
    while True:
        try:
            process_once(config)
        except Exception as exc:
            print(f"results poll error: {exc}")
        time.sleep(config.poll_interval_seconds)


if __name__ == "__main__":
    main()
