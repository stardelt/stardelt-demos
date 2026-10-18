#!/usr/bin/env bash
# Build stardelt-owned container images and load them into the kind cluster.
# kind has no registry, so images must be `kind load`-ed for pods to find them.
#
#   bash kind/build-images.sh                 # all three (the laptop default)
#   bash kind/build-images.sh airflow nova    # a subset (CI: see kind/demo.sh)
#
# The image names and tags must be the ones the platform's values and manifests
# expect, or the pods will pull (and fail) instead of using what was loaded:
#   airflow  stardelt/airflow:<tag>           helm-values/airflow.yaml
#   superset ghcr.io/stardelt/superset:<tag>  helm-values/superset.yaml
#   nova     stardelt/nova:<tag>              kind/up.sh retargets the Deployment
set -euo pipefail

CLUSTER="${CLUSTER:-stardelt}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PLATFORM_DIR="${STARDELT_PLATFORM_DIR:-$REPO_ROOT/../stardelt-platform}"
NOVA_DIR="${STARDELT_NOVA_DIR:-$REPO_ROOT/../stardelt-nova}"
IMAGE_TAG="${IMAGE_TAG:-dev}"

log() { printf '\033[1;34m▶\033[0m %s\n' "$*"; }
ok()  { printf '\033[1;32m✓\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31m✗\033[0m %s\n' "$*" >&2; exit 1; }

build_and_load() {
  local name="$1" image="$2" context="$3" dockerfile="$4"
  log "building $image"
  docker build -t "$image" -f "$dockerfile" "$context"
  log "loading $image into kind/$CLUSTER"
  kind load docker-image --name "$CLUSTER" "$image"
  ok "$image ready"
}

build_airflow() {
  # build context is repo root so dags/ resolves; Dockerfile in images/airflow/
  build_and_load airflow "stardelt/airflow:$IMAGE_TAG" "$REPO_ROOT" "$REPO_ROOT/images/airflow/Dockerfile"
}

build_superset() {
  # self-contained in stardelt-platform
  build_and_load superset "ghcr.io/stardelt/superset:$IMAGE_TAG" \
    "$PLATFORM_DIR/images/superset" "$PLATFORM_DIR/images/superset/Dockerfile"
}

build_nova() {
  # build context and Dockerfile in stardelt-nova
  build_and_load nova "stardelt/nova:$IMAGE_TAG" "$NOVA_DIR" "$NOVA_DIR/image/Dockerfile"
}

main() {
  local images=("$@")
  if (( ${#images[@]} == 0 )); then
    images=(airflow superset nova)
  fi

  local name
  for name in "${images[@]}"; do
    case "$name" in
      airflow|superset|nova) ;;
      *) fail "unknown image '$name' (expected: airflow, superset, nova)" ;;
    esac
  done

  [ -f "$PLATFORM_DIR/Makefile" ] || fail "no Makefile at $PLATFORM_DIR — set STARDELT_PLATFORM_DIR"
  for name in "${images[@]}"; do
    if [[ "$name" == "nova" && ! -d "$NOVA_DIR" ]]; then
      fail "stardelt-nova sibling repo not found at $NOVA_DIR — set STARDELT_NOVA_DIR"
    fi
    "build_$name"
  done
}

main "$@"
