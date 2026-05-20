#!/usr/bin/env bash
# Bring up the stardelt MVP on a local kind cluster.
# Idempotent: re-running advances past steps that already succeeded.
set -euo pipefail

CLUSTER="${CLUSTER:-stardelt}"
NAMESPACE="${NAMESPACE:-stardelt}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PLATFORM_DIR="${STARDELT_PLATFORM_DIR:-$(cd "$REPO_ROOT/../stardelt-platform" && pwd)}"
NOVA_DIR="${STARDELT_NOVA_DIR:-$(cd "$REPO_ROOT/../stardelt-nova" && pwd)}"

# Chart versions pinned 2026-05-16. Bump deliberately.
CNPG_CHART_VERSION="0.28.2"
SEAWEEDFS_CHART_VERSION="4.25.1"  # documented Ozone-alternative, fallback per plan
LAKEKEEPER_CHART_VERSION="0.11.0"
TRINO_CHART_VERSION="1.42.2"
AIRFLOW_CHART_VERSION="1.21.0"    # apache-airflow/airflow, appVersion 3.2.0
SUPERSET_CHART_VERSION="0.15.5"   # superset/superset, appVersion 5.0.0

log()  { printf '\033[1;34m▶\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m✓\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m!\033[0m %s\n' "$*" >&2; }
fail() { printf '\033[1;31m✗\033[0m %s\n' "$*" >&2; exit 1; }

ensure_cluster() {
  if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
    ok "kind cluster '$CLUSTER' already exists"
  else
    log "creating kind cluster '$CLUSTER'"
    mkdir -p "$REPO_ROOT/kind/.kind-data/ozone"
    # kind-config.yaml uses a relative hostPath (./.kind-data/ozone); cd into kind/ so it resolves
    (cd "$REPO_ROOT/kind" && kind create cluster --config "$REPO_ROOT/kind/kind-config.yaml")
    ok "kind cluster created"
  fi
  kubectl cluster-info --context "kind-$CLUSTER" >/dev/null
  kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
}

ensure_helm_repos() {
  log "adding helm repos"
  helm repo add cnpg           https://cloudnative-pg.github.io/charts          --force-update >/dev/null
  helm repo add seaweedfs      https://seaweedfs.github.io/seaweedfs/helm       --force-update >/dev/null
  helm repo add lakekeeper     https://lakekeeper.github.io/lakekeeper-charts/  --force-update >/dev/null
  helm repo add trino          https://trinodb.github.io/charts                 --force-update >/dev/null
  helm repo add apache-airflow https://airflow.apache.org                       --force-update >/dev/null
  helm repo add superset       https://apache.github.io/superset                --force-update >/dev/null
  helm repo update >/dev/null
  ok "helm repos ready"
}

ensure_helm_plugins() {
  if ! helm plugin list 2>/dev/null | grep -q '^stardelt-dedupe'; then
    log "installing helm post-renderer plugin (stardelt-dedupe)"
    helm plugin install "$PLATFORM_DIR/scripts/helm-plugins/stardelt-dedupe" >/dev/null
  fi
  ok "helm plugins ready"
}

install_cnpg() {
  log "installing CloudNative-PG operator (chart $CNPG_CHART_VERSION)"
  helm upgrade --install cnpg cnpg/cloudnative-pg \
    --version "$CNPG_CHART_VERSION" \
    --namespace cnpg-system --create-namespace \
    --wait --timeout 5m
  kubectl -n cnpg-system rollout status deploy/cnpg-cloudnative-pg --timeout=5m
  ok "CNPG operator ready"
}

install_seaweedfs() {
  log "installing SeaweedFS (chart $SEAWEEDFS_CHART_VERSION)"
  helm upgrade --install seaweedfs seaweedfs/seaweedfs \
    --version "$SEAWEEDFS_CHART_VERSION" \
    --namespace "$NAMESPACE" \
    -f "$PLATFORM_DIR/helm-values/seaweedfs.yaml" \
    --wait --timeout 5m
  kubectl -n "$NAMESPACE" rollout status sts/seaweedfs-master --timeout=2m
  kubectl -n "$NAMESPACE" rollout status sts/seaweedfs-volume --timeout=2m
  kubectl -n "$NAMESPACE" rollout status sts/seaweedfs-filer --timeout=2m
  kubectl -n "$NAMESPACE" rollout status sts/seaweedfs-s3 --timeout=2m
  ok "SeaweedFS ready"
}

bootstrap_s3() {
  log "applying S3 credentials Secret"
  kubectl apply -f "$PLATFORM_DIR/manifests/s3-credentials.example.yaml" >/dev/null
  ok "S3 credentials applied (bucket 'lakehouse' auto-created by chart)"
}

install_lakekeeper() {
  log "creating CNPG Cluster for Lakekeeper metadata"
  kubectl apply -f "$PLATFORM_DIR/manifests/cnpg-postgres.yaml" >/dev/null
  kubectl -n "$NAMESPACE" wait --for=condition=Ready --timeout=5m cluster/lakekeeper-pg
  ok "Postgres ready"
  log "installing Lakekeeper (chart $LAKEKEEPER_CHART_VERSION)"
  helm upgrade --install lakekeeper lakekeeper/lakekeeper \
    --version "$LAKEKEEPER_CHART_VERSION" \
    --namespace "$NAMESPACE" \
    -f "$PLATFORM_DIR/helm-values/lakekeeper.yaml" \
    --wait --timeout 5m
  kubectl -n "$NAMESPACE" rollout status deploy/lakekeeper --timeout=3m
  ok "Lakekeeper ready"
}

bootstrap_lakekeeper_warehouse() {
  log "bootstrapping Lakekeeper + 'warehouse' on Ozone S3"
  kubectl -n "$NAMESPACE" delete job lakekeeper-bootstrap --ignore-not-found=true >/dev/null
  kubectl apply -f "$PLATFORM_DIR/manifests/lakekeeper-bootstrap.yaml" >/dev/null
  kubectl -n "$NAMESPACE" wait --for=condition=complete --timeout=3m job/lakekeeper-bootstrap
  ok "Lakekeeper warehouse 'warehouse' ready"
}

install_trino() {
  log "installing Trino (chart $TRINO_CHART_VERSION)"
  helm upgrade --install trino trino/trino \
    --version "$TRINO_CHART_VERSION" \
    --namespace "$NAMESPACE" \
    -f "$PLATFORM_DIR/helm-values/trino.yaml" \
    --wait --timeout 5m
  kubectl -n "$NAMESPACE" rollout status deploy/trino-coordinator --timeout=3m
  kubectl -n "$NAMESPACE" rollout status deploy/trino-worker --timeout=3m
  ok "Trino ready"
}

build_stardelt_images() {
  log "building stardelt-owned container images"
  "$REPO_ROOT/kind/build-images.sh"
}

install_airflow() {
  log "installing Apache Airflow (chart $AIRFLOW_CHART_VERSION)"
  helm upgrade --install airflow apache-airflow/airflow \
    --version "$AIRFLOW_CHART_VERSION" \
    --namespace "$NAMESPACE" \
    -f "$PLATFORM_DIR/helm-values/airflow.yaml" \
    --timeout 10m
  kubectl -n "$NAMESPACE" rollout status deploy/airflow-scheduler  --timeout=5m
  kubectl -n "$NAMESPACE" rollout status deploy/airflow-api-server --timeout=5m
  ok "Airflow ready"
}

install_nova() {
  log "deploying stardelt Nova"
  kubectl apply -f "$PLATFORM_DIR/manifests/nova-deployment.yaml" >/dev/null
  kubectl -n "$NAMESPACE" rollout status deploy/nova --timeout=3m
  ok "Nova ready"
}

install_superset() {
  log "installing Apache Superset (chart $SUPERSET_CHART_VERSION)"
  helm upgrade --install superset superset/superset \
    --version "$SUPERSET_CHART_VERSION" \
    --namespace "$NAMESPACE" \
    -f "$PLATFORM_DIR/helm-values/superset.yaml" \
    --timeout 10m
  kubectl -n "$NAMESPACE" rollout status deploy/superset --timeout=5m
  ok "Superset ready"
}

main() {
  [ -d "$PLATFORM_DIR" ] || fail "stardelt-platform sibling repo not found at $PLATFORM_DIR — clone github.com/stardelt/stardelt-platform next to this repo"
  [ -d "$NOVA_DIR" ]     || fail "stardelt-nova sibling repo not found at $NOVA_DIR — clone github.com/stardelt/stardelt-nova next to this repo"

  ensure_cluster
  ensure_helm_repos
  ensure_helm_plugins
  install_cnpg
  install_seaweedfs
  bootstrap_s3
  install_lakekeeper
  bootstrap_lakekeeper_warehouse
  install_trino
  build_stardelt_images
  install_airflow
  install_superset
  install_nova
  log "stack up — run \`make smoke\` to validate, then trigger the nyc_taxi_load DAG"
}

main "$@"
