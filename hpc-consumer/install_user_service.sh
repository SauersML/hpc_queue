#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SERVICE_DIR="$HOME/.config/systemd/user"
SERVICE_FILE="$SERVICE_DIR/hpc_queue_consumer.service"
PYTHON_BIN="$(command -v python3)"

mkdir -p "$SERVICE_DIR"

cat > "$SERVICE_FILE" <<SERVICE
[Unit]
Description=HPC Queue Consumer
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$REPO_DIR
EnvironmentFile=$REPO_DIR/.env
ExecStart=$PYTHON_BIN $REPO_DIR/hpc-consumer/hpc_pull_consumer.py
Restart=always
RestartSec=3
NoNewPrivileges=true

[Install]
WantedBy=default.target
SERVICE

systemctl --user daemon-reload
systemctl --user enable --now hpc_queue_consumer.service

echo "Installed and started: $SERVICE_FILE"
echo "Status: systemctl --user status hpc_queue_consumer.service"
echo "Logs:   journalctl --user -u hpc_queue_consumer.service -f"
echo "If service stops after logout, run once: sudo loginctl enable-linger $USER"
