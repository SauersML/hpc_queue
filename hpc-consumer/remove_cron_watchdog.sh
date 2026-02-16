#!/usr/bin/env bash
set -euo pipefail

TMP_FILE="$(mktemp)"
(crontab -l 2>/dev/null || true) | grep -v 'HPC_QUEUE_WATCHDOG' > "$TMP_FILE"
crontab "$TMP_FILE"
rm -f "$TMP_FILE"

echo "removed cron watchdog"
