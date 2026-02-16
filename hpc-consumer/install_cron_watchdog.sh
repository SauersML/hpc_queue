#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENTRY="* * * * * $REPO_DIR/hpc-consumer/cron_watchdog.sh # HPC_QUEUE_WATCHDOG"

TMP_FILE="$(mktemp)"
(crontab -l 2>/dev/null || true) | grep -v 'HPC_QUEUE_WATCHDOG' > "$TMP_FILE"
echo "$ENTRY" >> "$TMP_FILE"
crontab "$TMP_FILE"
rm -f "$TMP_FILE"

echo "installed cron watchdog"
crontab -l | grep 'HPC_QUEUE_WATCHDOG' || true
