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

if [[ ! -f "build/apache-cassandra-7.0-SNAPSHOT.jar" ]]; then
  echo "[1/3] Building Apache Cassandra 7.0 HMA main jar..."
  ant _main-jar -Dno-resolver=true -Dno-build-accord=true -Dno-checkstyle=true
fi

echo "[2/3] Logging in to Docker Hub..."
printf "%s" "${DOCKERHUB_TOKEN}" | docker login -u "${DOCKERHUB_USERNAME}" --password-stdin

echo "[3/3] Building and pushing ${IMAGE_REPO}:${VERSION_TAG} (and :${EXTRA_TAG})..."
docker build --platform linux/amd64 \
  -t "${IMAGE_REPO}:${VERSION_TAG}" \
  -t "${IMAGE_REPO}:${EXTRA_TAG}" \
  -f Dockerfile .

docker push "${IMAGE_REPO}:${VERSION_TAG}"
docker push "${IMAGE_REPO}:${EXTRA_TAG}"

echo "Successfully pushed:"
echo "  - ${IMAGE_REPO}:${VERSION_TAG}"
echo "  - ${IMAGE_REPO}:${EXTRA_TAG}"
