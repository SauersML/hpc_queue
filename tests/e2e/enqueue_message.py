#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import urllib.request


def required(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def main() -> None:
    account_id = required("CLOUDFLARE_ACCOUNT_ID")
    queue_id = required("JOBS_QUEUE_ID")
    token = required("CLOUDFLARE_API_TOKEN")
    job_id = required("JOB_ID")

    url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/queues/{queue_id}/messages"
    payload = {
        "body": {
            "job_id": job_id,
            "task": "ci-e2e",
            "input": {"iterations": 3, "source": "github-actions"},
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
