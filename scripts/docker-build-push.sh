#!/usr/bin/env bash
set -euo pipefail

# Build and push Cassandra 7.0 HMA Docker image to Docker Hub.
# Automatically loads DOCKERHUB_USERNAME and DOCKERHUB_TOKEN from ~/.env if present.

if [[ -f "${HOME}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${HOME}/.env"
  set +a
fi

: "${DOCKERHUB_USERNAME:?DOCKERHUB_USERNAME must be set in environment or ~/.env}"
: "${DOCKERHUB_TOKEN:?DOCKERHUB_TOKEN must be set in environment or ~/.env}"

IMAGE_REPO="${DOCKERHUB_REPO:-${DOCKERHUB_USERNAME}/cassandra-hma}"
VERSION_TAG="${1:-v7.0.0-hma.2}"
EXTRA_TAG="${2:-latest}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${ROOT_DIR}"

if [[ -x "/usr/libexec/java_home" ]] && /usr/libexec/java_home -v 17 >/dev/null 2>&1; then
  export JAVA_HOME="$(/usr/libexec/java_home -v 17)"
fi

echo "[1/3] Building Apache Cassandra 7.0 HMA main jar..."
ant _main-jar -Dno-resolver=true -Dno-build-accord=true -Dno-checkstyle=true

echo "[2/3] Logging in to Docker Hub..."
printf "%s" "${DOCKERHUB_TOKEN}" | docker login -u "${DOCKERHUB_USERNAME}" --password-stdin

echo "[3/3] Building and pushing ${IMAGE_REPO}:${VERSION_TAG} (and :${EXTRA_TAG})..."
docker build --platform linux/amd64 \
  -t "${IMAGE_REPO}:${VERSION_TAG}" \
  -t "${IMAGE_REPO}:${EXTRA_TAG}" \
  -f Dockerfile .

docker push "${IMAGE_REPO}:${VERSION_TAG}"
docker push "${IMAGE_REPO}:${EXTRA_TAG}"

if [[ -f "doc/HMA_DOCKER_AND_CONFIG_GUIDE.md" ]] && command -v python3 >/dev/null 2>&1; then
  echo "[4/4] Syncing doc/HMA_DOCKER_AND_CONFIG_GUIDE.md to Docker Hub Overview..."
  python3 - <<'PY' || echo "Warning: could not update Docker Hub description via API (requires password or full-access PAT)"
import json, os, ssl, urllib.request

ctx = ssl.create_default_context(cafile="/etc/ssl/cert.pem") if os.path.exists("/etc/ssl/cert.pem") else ssl.create_default_context()
user = os.environ["DOCKERHUB_USERNAME"]
token = os.environ["DOCKERHUB_TOKEN"]
repo = os.environ.get("DOCKERHUB_REPO", f"{user}/cassandra-hma")
with open("doc/HMA_DOCKER_AND_CONFIG_GUIDE.md", "r", encoding="utf-8") as f:
    readme = f.read()

login_req = urllib.request.Request(
    "https://hub.docker.com/v2/users/login/",
    data=json.dumps({"username": user, "password": token}).encode("utf-8"),
    headers={"Content-Type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(login_req, timeout=15, context=ctx) as resp:
    jwt = json.loads(resp.read().decode("utf-8"))["token"]

patch_req = urllib.request.Request(
    f"https://hub.docker.com/v2/repositories/{repo}/",
    data=json.dumps({
        "description": "Apache Cassandra 7.0 HMA (Aerospike-Inspired Hybrid Memory Architecture & Fast-CAS Engine)",
        "full_description": readme,
    }).encode("utf-8"),
    headers={
        "Content-Type": "application/json",
        "Authorization": f"JWT {jwt}",
    },
    method="PATCH",
)
with urllib.request.urlopen(patch_req, timeout=15, context=ctx) as resp:
    print(f"Docker Hub Overview updated (HTTP {resp.status}).")
PY
fi

echo "Successfully pushed:"
echo "  - ${IMAGE_REPO}:${VERSION_TAG}"
echo "  - ${IMAGE_REPO}:${EXTRA_TAG}"
