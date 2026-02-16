#!/usr/bin/env python3
from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CONSUMER_SCRIPT = ROOT / "hpc-consumer" / "hpc_pull_consumer.py"
WORKER_PID_FILE = ROOT / "hpc-consumer" / "hpc_pull_consumer.pid"
SUPERVISOR_PID_FILE = ROOT / "hpc-consumer" / "hpc_supervisor.pid"
LOG_FILE = ROOT / "hpc-consumer" / "hpc_pull_consumer.log"

STOP_REQUESTED = False
CHILD_PROC: subprocess.Popen[str] | None = None


def _request_stop(_signum: int, _frame: object) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True
    if CHILD_PROC is not None and CHILD_PROC.poll() is None:
        CHILD_PROC.terminate()


def main() -> None:
    global CHILD_PROC

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    restart_delay = 2.0
    python_bin = sys.executable or "python3"

    SUPERVISOR_PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    SUPERVISOR_PID_FILE.write_text(str(os.getpid()), encoding="utf-8")

    while not STOP_REQUESTED:
        with LOG_FILE.open("a", encoding="utf-8") as log_fp:
            print(f"[supervisor] starting worker with {python_bin}", file=log_fp, flush=True)
            CHILD_PROC = subprocess.Popen(
                [python_bin, str(CONSUMER_SCRIPT)],
                cwd=str(ROOT),
                stdout=log_fp,
                stderr=subprocess.STDOUT,
                text=True,
                env=os.environ.copy(),
            )

        WORKER_PID_FILE.write_text(str(CHILD_PROC.pid), encoding="utf-8")
        rc = CHILD_PROC.wait()

        if STOP_REQUESTED:
            break

        with LOG_FILE.open("a", encoding="utf-8") as log_fp:
            print(
                f"[supervisor] worker exited rc={rc}; restarting in {restart_delay:.1f}s",
                file=log_fp,
                flush=True,
            )
        time.sleep(restart_delay)

    if WORKER_PID_FILE.exists():
        WORKER_PID_FILE.unlink(missing_ok=True)
    if SUPERVISOR_PID_FILE.exists():
        SUPERVISOR_PID_FILE.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
