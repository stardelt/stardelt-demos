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
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PLATFORM_DIR="${STARDELT_PLATFORM_DIR:-$(cd "$REPO_ROOT/../stardelt-platform" && pwd)}"
NOVA_DIR="${STARDELT_NOVA_DIR:-$(cd "$REPO_ROOT/../stardelt-nova" && pwd)}"

log()  { printf '\033[1;34m▶\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m✓\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m!\033[0m %s\n' "$*" >&2; }
fail() { printf '\033[1;31m✗\033[0m %s\n' "$*" >&2; exit 1; }

ensure_cluster() {
  if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
    ok "kind cluster '$CLUSTER' already exists"
  else
    log "creating kind cluster '$CLUSTER'"
    kind create cluster --config "$REPO_ROOT/kind/kind-config.yaml"
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

build_stardelt_images() {
  log "building stardelt-owned container images"
  "$REPO_ROOT/kind/build-images.sh"
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

  log "delegating install to stardelt-platform (make $target, APP_PROFILE=$APP_PROFILE)"
  if [[ "$target" == "install-core" && "$SLIM" == "1" ]]; then
    warn "SLIM=1 — applying the small-node overrides"
    make -C "$PLATFORM_DIR" "$target" NAMESPACE="$NAMESPACE" SLIM=1
  else
    make -C "$PLATFORM_DIR" "$target" NAMESPACE="$NAMESPACE"
  fi
  ok "stardelt '$APP_PROFILE' profile installed"
}

main() {
  [ -d "$PLATFORM_DIR" ]        || fail "stardelt-platform sibling repo not found at $PLATFORM_DIR — clone github.com/stardelt/stardelt-platform next to this repo"
  [ -f "$PLATFORM_DIR/Makefile" ] || fail "no Makefile at $PLATFORM_DIR — update the stardelt-platform checkout (needs install-core)"
  [ -d "$NOVA_DIR" ]            || fail "stardelt-nova sibling repo not found at $NOVA_DIR — clone github.com/stardelt/stardelt-nova next to this repo"

  ensure_cluster
  build_stardelt_images
  install_stack

  log "stack up ($APP_PROFILE) — run \`make smoke\` to validate"
  case "$APP_PROFILE" in
    core) log "core profile: no Airflow/Superset — use APP_PROFILE=full to include them" ;;
    full) log "trigger the demo pipeline with \`make airflow-trigger\`" ;;
  esac
}

main "$@"
