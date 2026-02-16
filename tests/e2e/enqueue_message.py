#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import urllib.request

DEFAULT_CF_ACCOUNT_ID = "59908b351c3a3321ff84dd2d78bf0b42"
DEFAULT_CF_JOBS_QUEUE_ID = "f52e2e6bb569425894ede9141e9343a5"


def required(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def main() -> None:
    account_id = DEFAULT_CF_ACCOUNT_ID
    queue_id = DEFAULT_CF_JOBS_QUEUE_ID
    token = required("CLOUDFLARE_API_TOKEN")
    job_id = required("JOB_ID")

    url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/queues/{queue_id}/messages"
    payload = {
        "body": {
            "job_id": job_id,
            "input": {
                "exec_mode": "host",
                "command": "echo cloudflare-e2e-ok",
                "source": "github-actions",
            },
        }
    }

    req = urllib.request.Request(
        url=url,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )

    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    if not data.get("success"):
        raise RuntimeError(f"enqueue failed: {data}")

    print(f"enqueued {job_id}")


if __name__ == "__main__":
    main()
