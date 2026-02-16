#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TARGET="$REPO_DIR/hpc-consumer/hpc_pull_consumer.py"

if pgrep -f "$TARGET" >/dev/null 2>&1; then
  exit 0
fi

"$REPO_DIR/hpc-consumer/start_consumer.sh"
