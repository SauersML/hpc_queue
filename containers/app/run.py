#!/usr/bin/env python3
"""Container entrypoint for hpc_queue jobs.

Reads /work/input.json and writes /work/output.json.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


INPUT_PATH = Path("/work/input.json")
OUTPUT_PATH = Path("/work/output.json")


def main() -> None:
    payload = json.loads(INPUT_PATH.read_text(encoding="utf-8"))
    job_id = str(payload.get("job_id", "unknown"))
    task = str(payload.get("task", "unknown"))
    data = payload.get("input", {})

    result = {
        "job_id": job_id,
        "task": task,
        "status": "completed",
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "result": {
            "echo": data,
            "message": "container execution completed",
        },
    }

    OUTPUT_PATH.write_text(json.dumps(result, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
