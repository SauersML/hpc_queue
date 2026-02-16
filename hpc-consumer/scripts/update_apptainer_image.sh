#!/usr/bin/env bash
set -euo pipefail

# Keep local SIF fresh without unnecessary pulls:
# - Resolve remote digest for APPTAINER_OCI_REF
# - Skip pull when local digest matches
# - Pull only when digest changed or image missing

REPO_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_DIR"

set -a
source .env
set +a

: "${APPTAINER_BIN:=apptainer}"
: "${APPTAINER_IMAGE:=$REPO_DIR/runtime/hpc-queue-runtime.sif}"
: "${APPTAINER_OCI_REF:=ghcr.io/sauersml/hpc-queue-runtime:latest}"
: "${PYTHON_BIN:=python3}"

mkdir -p "$(dirname "$APPTAINER_IMAGE")"

TMP_IMAGE="${APPTAINER_IMAGE}.tmp"
DIGEST_FILE="${APPTAINER_IMAGE}.digest"

resolve_remote_digest() {
  "$PYTHON_BIN" - "$APPTAINER_OCI_REF" <<'PY'
import base64
import json
import os
import sys
import urllib.parse
import urllib.request
import urllib.error

ref = sys.argv[1]
if ref.startswith("docker://"):
    ref = ref[len("docker://"):]
if "/" not in ref:
    raise SystemExit(f"invalid OCI ref: {ref}")

registry, remainder = ref.split("/", 1)
if "@" in remainder:
    _repo, digest = remainder.split("@", 1)
    print(digest)
    raise SystemExit(0)

if ":" in remainder.rsplit("/", 1)[-1]:
    repo, tag = remainder.rsplit(":", 1)
else:
    repo, tag = remainder, "latest"

scope = urllib.parse.quote(f"repository:{repo}:pull", safe="")
token_url = f"https://{registry}/token?service={registry}&scope={scope}"
token_req = urllib.request.Request(token_url, method="GET")

ghcr_token = os.getenv("GHCR_TOKEN", "")
ghcr_user = os.getenv("GHCR_USERNAME", "") or "oauth2"
if ghcr_token:
    basic = base64.b64encode(f"{ghcr_user}:{ghcr_token}".encode()).decode()
    token_req.add_header("Authorization", f"Basic {basic}")

try:
    with urllib.request.urlopen(token_req, timeout=30) as resp:
        token_data = json.loads(resp.read().decode("utf-8"))
except urllib.error.HTTPError as exc:
    if exc.code in (401, 403):
        raise SystemExit(
            "registry token unauthorized (401/403); set GHCR_TOKEN (and optionally GHCR_USERNAME) "
            "if the image is private"
        )
    raise
token = token_data.get("token") or token_data.get("access_token")
if not token:
    raise SystemExit("failed to obtain registry token")

manifest_url = f"https://{registry}/v2/{repo}/manifests/{tag}"
manifest_req = urllib.request.Request(manifest_url, method="GET")
manifest_req.add_header("Authorization", f"Bearer {token}")
manifest_req.add_header(
    "Accept",
    ",".join(
        [
            "application/vnd.oci.image.index.v1+json",
            "application/vnd.oci.image.manifest.v1+json",
            "application/vnd.docker.distribution.manifest.list.v2+json",
            "application/vnd.docker.distribution.manifest.v2+json",
        ]
    ),
)

try:
    with urllib.request.urlopen(manifest_req, timeout=30) as resp:
        digest = resp.headers.get("Docker-Content-Digest", "").strip()
except urllib.error.HTTPError as exc:
    if exc.code in (401, 403):
        raise SystemExit(
            "manifest unauthorized (401/403); set GHCR_TOKEN (and optionally GHCR_USERNAME) "
            "if the image is private"
        )
    raise
if not digest:
    raise SystemExit("failed to resolve remote digest")
print(digest)
PY
}

REMOTE_DIGEST=""
if ! REMOTE_DIGEST="$(resolve_remote_digest)"; then
  echo "warning: could not resolve remote digest for $APPTAINER_OCI_REF" >&2
  if [[ -f "$APPTAINER_IMAGE" ]]; then
    echo "warning: using existing local image at $APPTAINER_IMAGE" >&2
    exit 0
  fi

  echo "no local image found; attempting direct pull without digest precheck" >&2
  if "$APPTAINER_BIN" pull --force "$TMP_IMAGE" "docker://$APPTAINER_OCI_REF"; then
    mv "$TMP_IMAGE" "$APPTAINER_IMAGE"
    echo "pulled image without digest precheck: $APPTAINER_IMAGE"
    exit 0
  fi

  echo "error: failed to refresh image." >&2
  echo "if image is private, add GHCR_TOKEN to .env (and GHCR_USERNAME if needed)." >&2
  exit 1
fi

LOCAL_DIGEST=""
if [[ -f "$DIGEST_FILE" ]]; then
  LOCAL_DIGEST="$(cat "$DIGEST_FILE" || true)"
fi

if [[ -f "$APPTAINER_IMAGE" && "$LOCAL_DIGEST" == "$REMOTE_DIGEST" ]]; then
  echo "image up-to-date ($REMOTE_DIGEST), skipping pull"
  exit 0
fi

echo "digest changed ($LOCAL_DIGEST -> $REMOTE_DIGEST), pulling $APPTAINER_OCI_REF"
"$APPTAINER_BIN" pull --force "$TMP_IMAGE" "docker://$APPTAINER_OCI_REF"
mv "$TMP_IMAGE" "$APPTAINER_IMAGE"
printf "%s\n" "$REMOTE_DIGEST" > "$DIGEST_FILE"
echo "updated $APPTAINER_IMAGE ($REMOTE_DIGEST)"
