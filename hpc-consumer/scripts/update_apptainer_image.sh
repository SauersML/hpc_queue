#!/usr/bin/env bash
set -euo pipefail

# Pull latest GHCR image and convert to local SIF for fast reuse.
# Requires APPTAINER_IMAGE and APPTAINER_OCI_REF in .env.

REPO_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_DIR"

set -a
source .env
set +a

: "${APPTAINER_BIN:=apptainer}"
: "${APPTAINER_IMAGE:=/Users/user/hpc_queue/runtime/hpc-queue-runtime.sif}"
: "${APPTAINER_OCI_REF:=ghcr.io/sauersml/hpc-queue-runtime:latest}"

mkdir -p "$(dirname "$APPTAINER_IMAGE")"

TMP_IMAGE="${APPTAINER_IMAGE}.tmp"

echo "Pulling $APPTAINER_OCI_REF -> $TMP_IMAGE"
"$APPTAINER_BIN" pull --force "$TMP_IMAGE" "docker://$APPTAINER_OCI_REF"

mv "$TMP_IMAGE" "$APPTAINER_IMAGE"
echo "Updated $APPTAINER_IMAGE"
