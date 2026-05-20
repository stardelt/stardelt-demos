#!/usr/bin/env bash
# Build stardelt-owned container images and load them into the kind cluster.
# kind has no registry, so images must be `kind load`-ed for pods to find them.
set -euo pipefail

CLUSTER="${CLUSTER:-stardelt}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PLATFORM_DIR="${STARDELT_PLATFORM_DIR:-$(cd "$REPO_ROOT/../stardelt-platform" && pwd)}"
NOVA_DIR="${STARDELT_NOVA_DIR:-$(cd "$REPO_ROOT/../stardelt-nova" && pwd)}"
IMAGE_TAG="${IMAGE_TAG:-dev}"

log() { printf '\033[1;34m▶\033[0m %s\n' "$*"; }
ok()  { printf '\033[1;32m✓\033[0m %s\n' "$*"; }

build_and_load() {
  local name="$1" context="$2" dockerfile="$3"
  local image="stardelt/$name:$IMAGE_TAG"
  log "building $image"
  docker build -t "$image" -f "$dockerfile" "$context"
  log "loading $image into kind/$CLUSTER"
  kind load docker-image --name "$CLUSTER" "$image"
  ok "$image ready"
}

main() {
  # airflow: build context is repo root so dags/ resolves; Dockerfile in images/airflow/
  build_and_load airflow  "$REPO_ROOT"                              "$REPO_ROOT/images/airflow/Dockerfile"
  # superset: self-contained in stardelt-platform
  build_and_load superset "$PLATFORM_DIR/images/superset"          "$PLATFORM_DIR/images/superset/Dockerfile"
  # nova: build context and Dockerfile in stardelt-nova
  build_and_load nova     "$NOVA_DIR"                               "$NOVA_DIR/image/Dockerfile"
}

main "$@"
