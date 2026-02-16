#!/usr/bin/env bash
set -euo pipefail

ACTION="${1:-status}"
case "$ACTION" in
  start|stop|restart|status)
    systemctl --user "$ACTION" hpc_queue_consumer.service
    ;;
  logs)
    journalctl --user -u hpc_queue_consumer.service -f
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|status|logs}"
    exit 1
    ;;
esac
