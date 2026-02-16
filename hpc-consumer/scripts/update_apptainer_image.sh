#!/usr/bin/env bash
set -euo pipefail

# Keep local SIF fresh without unnecessary downloads:
# - Fetch remote sha256 for release-published SIF
# - Skip download when local digest matches
# - Download + verify only when digest changed or image missing

REPO_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_DIR"

APPTAINER_IMAGE="$REPO_DIR/runtime/hpc-queue-runtime.sif"
APPTAINER_SIF_URL="https://github.com/SauersML/hpc_queue/releases/download/sif-latest/hpc-queue-runtime.sif"
APPTAINER_SIF_SHA256_URL="https://github.com/SauersML/hpc_queue/releases/download/sif-latest/hpc-queue-runtime.sif.sha256"

mkdir -p "$(dirname "$APPTAINER_IMAGE")"

TMP_IMAGE="${APPTAINER_IMAGE}.tmp"
DIGEST_FILE="${APPTAINER_IMAGE}.digest"

fetch_remote_digest() {
  local sha_line
  sha_line="$(curl -fsSL "$APPTAINER_SIF_SHA256_URL")"
  echo "$sha_line" | awk '{print $1}'
}

REMOTE_DIGEST=""
if ! REMOTE_DIGEST="$(fetch_remote_digest)"; then
  echo "warning: could not read remote SIF digest from $APPTAINER_SIF_SHA256_URL" >&2
  if [[ -f "$APPTAINER_IMAGE" ]]; then
    echo "warning: using existing local image at $APPTAINER_IMAGE" >&2
    exit 0
  fi
  echo "error: no local image found and remote SIF digest unavailable" >&2
  exit 1
fi

if [[ -z "$REMOTE_DIGEST" ]]; then
  echo "error: empty remote digest from $APPTAINER_SIF_SHA256_URL" >&2
  exit 1
fi

LOCAL_DIGEST=""
if [[ -f "$DIGEST_FILE" ]]; then
  LOCAL_DIGEST="$(cat "$DIGEST_FILE" || true)"
fi

if [[ -f "$APPTAINER_IMAGE" && "$LOCAL_DIGEST" == "$REMOTE_DIGEST" ]]; then
  echo "image up-to-date ($REMOTE_DIGEST), skipping download"
  exit 0
fi

echo "digest changed ($LOCAL_DIGEST -> $REMOTE_DIGEST), downloading SIF"
if ! curl -fsSL "$APPTAINER_SIF_URL" -o "$TMP_IMAGE"; then
  if [[ -f "$APPTAINER_IMAGE" ]]; then
    echo "warning: failed to download new SIF ($APPTAINER_SIF_URL); keeping existing local image" >&2
    exit 0
  fi
  echo "error: failed to download SIF and no local image exists" >&2
  exit 1
fi

DOWNLOADED_DIGEST="$(sha256sum "$TMP_IMAGE" | awk '{print $1}')"
if [[ "$DOWNLOADED_DIGEST" != "$REMOTE_DIGEST" ]]; then
  echo "error: SIF digest mismatch ($DOWNLOADED_DIGEST != $REMOTE_DIGEST)" >&2
  rm -f "$TMP_IMAGE"
  if [[ -f "$APPTAINER_IMAGE" ]]; then
    echo "warning: keeping existing local image after digest mismatch" >&2
    exit 0
  fi
  exit 1
fi

mv "$TMP_IMAGE" "$APPTAINER_IMAGE"
printf "%s\n" "$REMOTE_DIGEST" > "$DIGEST_FILE"
echo "updated $APPTAINER_IMAGE ($REMOTE_DIGEST)"
