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

mkdir -p "$(dirname "$APPTAINER_IMAGE")"

TMP_IMAGE="${APPTAINER_IMAGE}.tmp"
DIGEST_FILE="${APPTAINER_IMAGE}.digest"

resolve_remote_digest() {
  python3 - "$APPTAINER_OCI_REF" <<'PY'
import base64
import json
import os
import sys
import urllib.parse
import urllib.request

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

with urllib.request.urlopen(token_req, timeout=30) as resp:
    token_data = json.loads(resp.read().decode("utf-8"))
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

with urllib.request.urlopen(manifest_req, timeout=30) as resp:
    digest = resp.headers.get("Docker-Content-Digest", "").strip()
if not digest:
    raise SystemExit("failed to resolve remote digest")
print(digest)
PY
}

REMOTE_DIGEST="$(resolve_remote_digest)"
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
