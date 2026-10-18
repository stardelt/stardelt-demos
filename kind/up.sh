#!/usr/bin/env bash
# Bring up stardelt on a local kind cluster.
#
# This script is a thin kind-specific wrapper around stardelt-platform. It owns
# only the steps that are about *kind* (cluster, namespace, locally-built
# images) and delegates the install itself to the platform Makefile, so the
# laptop path cannot drift from the platform wiring (WP-17).
#
#   APP_PROFILE=core (default)  ->  make -C <platform> install-core
#                                   storage + catalog + query + Nova
#   APP_PROFILE=full            ->  make -C <platform> install
#                                   the above plus Airflow + Superset
#   SLIM=1                      ->  forward SLIM=1 to install-core (small nodes)
#   STARDELT_IMAGES=build|none|"<names>"
#                               ->  build|kind-load the stardelt images:
#                                   `build` = all three (default), `none` = skip
#                                   (stardelt-nova is then not needed; Nova will
#                                   not run — see kind/gate.sh), or a list of
#                                   `airflow superset nova` to build a subset
#                                   (kind/demo.sh needs the first two only).
#   KIND_NODE_IMAGE=kindest/node:vX.Y.Z
#                               ->  pin the kind node image (default: whatever
#                                   the installed kind ships)
#
# The primary development environment is the Hetzner k3s cluster in
# stardelt-platform/dev/hetzner-k3s; kind is the secondary/laptop path.
#
# Idempotent: re-running advances past steps that already succeeded.
set -euo pipefail

CLUSTER="${CLUSTER:-stardelt}"
NAMESPACE="${NAMESPACE:-stardelt}"
APP_PROFILE="${APP_PROFILE:-core}"
SLIM="${SLIM:-}"
IMAGE_TAG="${IMAGE_TAG:-dev}"
STARDELT_IMAGES="${STARDELT_IMAGES:-build}"
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PLATFORM_DIR="${STARDELT_PLATFORM_DIR:-$REPO_ROOT/../stardelt-platform}"
NOVA_DIR="${STARDELT_NOVA_DIR:-$REPO_ROOT/../stardelt-nova}"

log()  { printf '\033[1;34m▶\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m✓\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m!\033[0m %s\n' "$*" >&2; }
fail() { printf '\033[1;31m✗\033[0m %s\n' "$*" >&2; exit 1; }

ensure_cluster() {
  if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
    ok "kind cluster '$CLUSTER' already exists"
  else
    local image_args=()
    if [[ -n "$KIND_NODE_IMAGE" ]]; then
      image_args=(--image "$KIND_NODE_IMAGE")
      log "creating kind cluster '$CLUSTER' (node image $KIND_NODE_IMAGE)"
    else
      log "creating kind cluster '$CLUSTER'"
    fi
    kind create cluster --config "$REPO_ROOT/kind/kind-config.yaml" --wait 180s "${image_args[@]}"
    ok "kind cluster created"
  fi
  kubectl cluster-info --context "kind-$CLUSTER" >/dev/null
  # The platform Makefile installs against the *current* kube-context, which may
  # point at another cluster (e.g. the Hetzner k3s dev environment). Pin it.
  kubectl config use-context "kind-$CLUSTER" >/dev/null
  ok "kube-context kind-$CLUSTER selected"
  kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  ok "namespace '$NAMESPACE' ready"
}

# ---------------------------------------------------------------------------
# stardelt-owned images
# ---------------------------------------------------------------------------

# Resolve STARDELT_IMAGES into the list of image names to build:
#   build -> all three, none -> none, otherwise a list ("airflow superset nova",
#   comma or space separated). kind/demo.sh uses a list to skip nova.
IMAGE_NAMES=()
resolve_images() {
  case "$STARDELT_IMAGES" in
    build)    IMAGE_NAMES=(airflow superset nova) ;;
    none|"")  IMAGE_NAMES=() ;;
    *)        read -r -a IMAGE_NAMES <<<"${STARDELT_IMAGES//,/ }" ;;
  esac
  local name
  for name in ${IMAGE_NAMES[@]+"${IMAGE_NAMES[@]}"}; do
    case "$name" in
      airflow|superset|nova) ;;
      *) fail "unknown STARDELT_IMAGES entry '$name' (expected 'build', 'none' or a list of: airflow superset nova)" ;;
    esac
  done
}

images_include() {  # images_include <name>
  local name
  for name in ${IMAGE_NAMES[@]+"${IMAGE_NAMES[@]}"}; do
    [[ "$name" == "$1" ]] && return 0
  done
  return 1
}

build_stardelt_images() {
  log "building stardelt-owned container images: ${IMAGE_NAMES[*]}"
  "$REPO_ROOT/kind/build-images.sh" "${IMAGE_NAMES[@]}"
  ok "images loaded into kind/$CLUSTER"
}

install_stack() {
  local target
  case "$APP_PROFILE" in
    core) target="install-core" ;;
    full) target="install" ;;
    *)    fail "unknown APP_PROFILE '$APP_PROFILE' (expected 'core' or 'full')" ;;
  esac

  command -v make >/dev/null || fail "'make' is required to drive the stardelt-platform install"

  # The platform's install targets generate their own credentials (scripts/s3-secrets.sh).

  log "delegating install to stardelt-platform (make $target, APP_PROFILE=$APP_PROFILE)"
  if [[ "$target" == "install-core" && "$SLIM" == "1" ]]; then
    warn "SLIM=1 — applying the small-node overrides"
    make -C "$PLATFORM_DIR" "$target" NAMESPACE="$NAMESPACE" SLIM=1
  else
    make -C "$PLATFORM_DIR" "$target" NAMESPACE="$NAMESPACE"
  fi
  ok "stardelt '$APP_PROFILE' profile installed"
}

# The platform manifest points Nova at ghcr.io with a pull secret. kind has no
# registry, so retarget the Deployment at the locally built, kind-loaded image.
# Only when Nova is one of the images this run built.
pin_kind_images() {
  images_include nova || return 0
  local image="stardelt/nova:$IMAGE_TAG"
  if kubectl -n "$NAMESPACE" get deploy/nova >/dev/null 2>&1; then
    log "pinning nova to the locally built image ($image)"
    kubectl -n "$NAMESPACE" set image deploy/nova "nova=$image" >/dev/null
    kubectl -n "$NAMESPACE" patch deploy/nova --type=merge \
      -p '{"spec":{"template":{"spec":{"imagePullPolicy":"IfNotPresent","imagePullSecrets":null}}}}' >/dev/null
    kubectl -n "$NAMESPACE" rollout status deploy/nova --timeout=180s >/dev/null
    ok "nova running from $image"
  fi
}

main() {
  resolve_images

  [ -d "$PLATFORM_DIR" ]        || fail "stardelt-platform sibling repo not found at $PLATFORM_DIR — clone github.com/stardelt/stardelt-platform next to this repo"
  [ -f "$PLATFORM_DIR/Makefile" ] || fail "no Makefile at $PLATFORM_DIR — update the stardelt-platform checkout (needs install-core)"
  if images_include nova; then
    [ -d "$NOVA_DIR" ] || fail "stardelt-nova sibling repo not found at $NOVA_DIR — clone github.com/stardelt/stardelt-nova next to this repo (or drop nova from STARDELT_IMAGES)"
  fi

  ensure_cluster
  if (( ${#IMAGE_NAMES[@]} > 0 )); then
    build_stardelt_images
  else
    warn "STARDELT_IMAGES=none — skipping the stardelt-owned images; Nova will not run from ghcr.io (for CI: see kind/gate.sh)"
  fi
  install_stack
  pin_kind_images

  log "stack up ($APP_PROFILE) — run \`make smoke\` to validate"
  case "$APP_PROFILE" in
    core) log "core profile: no Airflow/Superset — use APP_PROFILE=full to include them" ;;
    full) log "trigger the demo pipeline with \`make airflow-trigger\` (or run the whole leg: \`make demo\`)" ;;
  esac
}

main "$@"
