#!/usr/bin/env python3
"""Container entrypoint for hpc_queue jobs.

Reads /work/input.json and writes /work/output.json.
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


INPUT_PATH = Path("/work/input.json")
OUTPUT_PATH = Path("/work/output.json")
STDOUT_PATH = Path("/work/stdout.log")
STDERR_PATH = Path("/work/stderr.log")
DEFAULT_CONTAINER_WORKDIR = Path("/gnomon")


def main() -> None:
    payload = json.loads(INPUT_PATH.read_text(encoding="utf-8"))
    job_id = str(payload.get("job_id", "unknown"))
    data = payload.get("input", {})
    command = str(data.get("command", "echo no command provided"))
    requested_workdir = str(data.get("workdir", "")).strip()
    if requested_workdir:
        workdir = Path(requested_workdir)
    elif DEFAULT_CONTAINER_WORKDIR.exists():
        workdir = DEFAULT_CONTAINER_WORKDIR
    else:
        workdir = Path("/")

    started_at = datetime.now(timezone.utc).isoformat()
    proc = subprocess.run(
        command,
        shell=True,
        capture_output=True,
        text=True,
        cwd=str(workdir),
    )
    finished_at = datetime.now(timezone.utc).isoformat()

    STDOUT_PATH.write_text(proc.stdout, encoding="utf-8")
    STDERR_PATH.write_text(proc.stderr, encoding="utf-8")

    result = {
        "job_id": job_id,
        "status": "completed" if proc.returncode == 0 else "failed",
        "started_at": started_at,
        "finished_at": finished_at,
        "exit_code": proc.returncode,
        "result": {
            "command": command,
            "workdir": str(workdir),
            "stdout_path": str(STDOUT_PATH),
            "stderr_path": str(STDERR_PATH),
        },
    }

    OUTPUT_PATH.write_text(json.dumps(result, indent=2), encoding="utf-8")
    raise SystemExit(proc.returncode)


if __name__ == "__main__":
    main()
