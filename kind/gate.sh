#!/usr/bin/env bash
# The WP-02 verification gate: prove stardelt installs and works on a clean
# cluster, with no human input.
#
#   kind cluster → stardelt-platform `make install-core SLIM=1`
#     → CloudNative-PG (Lakekeeper's metadata store)
#     → SeaweedFS (a real S3 object store)
#     → Lakekeeper (a real Iceberg REST catalog, warehouse on s3://lakehouse)
#     → Trino (a real query engine, catalog `warehouse`)
#   → kind/smoke.sh: CREATE SCHEMA/TABLE, INSERT, SELECT — the round-trip
#   → the row is still readable after the object store restarts, and the table
#     carries a real Iceberg snapshot.
#
# Scope and exclusions are documented in README.md ("Verification gate") and in
# .github/workflows/verify.yml. Short version: Nova, Airflow, Superset,
# JupyterHub, ingress/TLS, OIDC and the NYC-taxi DAG are NOT covered — this gate
# is the storage + catalog + query data path.
#
# Honest by construction: every phase is asserted, the first failure exits
# non-zero and stops the gate, and no phase is skipped to keep a run green.
# Diagnostics live in kind/diagnostics.sh; the workflow runs it on failure.
#
# Locally (needs docker + kind + kubectl + helm and a stardelt-platform sibling):
#   bash kind/gate.sh
set -euo pipefail

CLUSTER="${CLUSTER:-stardelt}"
NAMESPACE="${NAMESPACE:-stardelt}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# The gate installs the core profile with the small-node overrides, and does not
# build the stardelt-owned images (CI does not build them; see the exclusions in
# the README). Everything else is the platform installer under test.
export APP_PROFILE=core
export SLIM=1
export STARDELT_IMAGES=none

# Total wall-clock budget for the phases below. Bounded on purpose: a stuck
# gate must fail loudly inside the workflow's job timeout, not hang until the
# runner kills it. Override for a slow laptop: GATE_BUDGET_SECONDS=3600.
GATE_BUDGET_SECONDS="${GATE_BUDGET_SECONDS:-1800}"

EXPECTED_ROW="1,hello-iceberg-on-s3"

# The workloads the core profile (v-values of stardelt-platform) must produce.
# An explicit list on purpose: if a chart bump renames one of these, the gate
# goes red and names it, instead of waiting for nothing.
WORKLOADS=(
  deployment/cnpg-cloudnative-pg
  statefulset/seaweedfs-master
  statefulset/seaweedfs-volume
  statefulset/seaweedfs-filer
  deployment/seaweedfs-s3
  deployment/lakekeeper
  deployment/trino-coordinator
  deployment/trino-worker
)

log()  { printf '\033[1;34m▶\033[0m [%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }
ok()   { printf '\033[1;32m✓\033[0m [%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }
warn() { printf '\033[1;33m!\033[0m [%s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }
fail() { printf '\033[1;31m✗\033[0m [%s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; exit 1; }

trap 'printf "\033[1;31m✗\033[0m gate aborted at line %s: %s\n" "$LINENO" "$BASH_COMMAND" >&2' ERR

DEADLINE=$(( SECONDS + GATE_BUDGET_SECONDS ))
PHASE_LOG=()

# bounded <cap_seconds> — the smaller of <cap> and what is left of the budget.
bounded() {
  local cap="$1" left=$(( DEADLINE - SECONDS ))
  if (( left < cap )); then cap=$left; fi
  if (( cap < 5 )); then cap=0; fi
  printf '%s' "$cap"
}

# wait_seconds <label> <cap> <kubectl wait/rollout ...> — bounded, then loud.
wait_seconds() {
  local label="$1" cap="$2"
  shift 2
  local t
  t="$(bounded "$cap")"
  (( t > 0 )) || fail "budget of ${GATE_BUDGET_SECONDS}s exhausted before waiting for '$label'"
  log "waiting for $label (up to ${t}s)"
  "$@" --timeout="${t}s" || fail "'$label' did not become ready within ${t}s"
  ok "$label ready"
}

# phase <name> <command...> — run one asserted phase, time it, stop on failure.
phase() {
  local name="$1" start=$SECONDS
  shift
  log "phase: $name"
  if ! "$@"; then
    PHASE_LOG+=("$name FAILED after $(( SECONDS - start ))s")
    fail "phase '$name' failed — see the diagnostics step of this run"
  fi
  PHASE_LOG+=("$name ok ($(( SECONDS - start ))s)")
  ok "phase '$name' done in $(( SECONDS - start ))s"
}

# ---------------------------------------------------------------------------
# Phases
# ---------------------------------------------------------------------------

# 1. cluster + install: kind cluster, namespace, platform `install-core SLIM=1`.
phase_install() {
  bash "$REPO_ROOT/kind/up.sh"
}

# 2. scope: the core profile also applies the Nova Deployment, which needs the
#    stardelt/nova image that the laptop path builds locally (kind/build-images.sh).
#    CI builds no stardelt images, so Nova is out of this gate's scope: drop the
#    Deployment (and its pods) so the gate reports on the data path it covers.
#    Nothing else the platform installed is touched.
phase_scope() {
  kubectl -n "$NAMESPACE" delete deployment/nova --ignore-not-found --wait=true
  kubectl -n "$NAMESPACE" delete pods -l app.kubernetes.io/name=nova --ignore-not-found --wait=true --timeout=120s
  ok "Nova excluded from the gate (no image built in CI; not part of the data path)"
}

# 3. the install really produced the releases the core profile promises.
phase_releases() {
  local expected=(cnpg seaweedfs lakekeeper trino)
  local rel status
  for rel in "${expected[@]}"; do
    status="$(helm -n "$NAMESPACE" list --all --filter "^${rel}\$" -o json | jq -r '.[0].status // "missing"')"
    [[ "$status" == "deployed" ]] || fail "helm release '$rel' is '$status' (expected 'deployed')"
    ok "helm release $rel: deployed"
  done
}

# 4. readiness of the data path, in dependency order, each wait bounded.
phase_ready() {
  # The bootstrap Job creates the `warehouse` on s3://lakehouse. It is checked
  # first because it carries ttlSecondsAfterFinished=600: waiting for it late in
  # a slow run could race its garbage collection. If it is already gone, the
  # round-trip below is the real proof that the warehouse exists.
  if kubectl -n "$NAMESPACE" get job/lakekeeper-bootstrap >/dev/null 2>&1; then
    wait_seconds "job/lakekeeper-bootstrap" 240 \
      kubectl -n "$NAMESPACE" wait --for=condition=complete job/lakekeeper-bootstrap
  else
    warn "job/lakekeeper-bootstrap is gone (ttlSecondsAfterFinished=600) — the round-trip below proves the warehouse exists"
  fi

  local w
  for w in "${WORKLOADS[@]}"; do
    kubectl -n "$NAMESPACE" get "$w" >/dev/null 2>&1 || fail "expected workload '$w' is not installed by the core profile"
    wait_seconds "$w" 420 kubectl -n "$NAMESPACE" rollout status "$w"
  done

  # CNPG's Cluster condition (Lakekeeper's Postgres). install-core waits for it
  # too; re-asserting here means a silently skipped install cannot pass the gate.
  wait_seconds "cluster/lakekeeper-pg" 120 \
    kubectl -n "$NAMESPACE" wait --for=condition=Ready cluster/lakekeeper-pg
}

# 5. no workload in the namespace may be crashlooping or unpullable. Catches the
#    "installed but broken" cases the rollouts above cannot see (e.g. a pod that
#    never schedules, an image that cannot be pulled).
phase_no_broken_pods() {
  local bad
  bad="$(kubectl -n "$NAMESPACE" get pods -o json | jq -r '
    .items[]
    | select(
        .status.phase == "Failed"
        or ([ .status.containerStatuses[]?
              | select(.state.waiting.reason? != null)
              | select(.state.waiting.reason | test("CrashLoopBackOff|ImagePullBackOff|ErrImagePull|CreateContainerConfigError|CreateContainerError|RunContainerError"))
            ] | length) > 0
      )
    | "\(.metadata.name) phase=\(.status.phase) reason=\([.status.containerStatuses[]?.state.waiting.reason // "-"] | join(","))"
  ')"
  if [[ -n "$bad" ]]; then
    printf '%s\n' "$bad" >&2
    fail "pods in the namespace '$NAMESPACE' are failing"
  fi
  ok "no pod in '$NAMESPACE' is crashlooping or unpullable"
}

# 6. the acceptance script of this repo: write + read through Trino → Iceberg → S3.
phase_smoke() {
  NAMESPACE="$NAMESPACE" bash "$REPO_ROOT/kind/smoke.sh"
}

# ---------------------------------------------------------------------------
# Trino helpers
# ---------------------------------------------------------------------------

trino_exec() {  # trino_exec <sql> — run one statement in the Trino coordinator
  local coord
  coord="$(kubectl -n "$NAMESPACE" get pod \
    -l 'app.kubernetes.io/name=trino,app.kubernetes.io/component=coordinator' \
    -o jsonpath='{.items[0].metadata.name}')"
  [[ -n "$coord" ]] || fail "no Trino coordinator pod in namespace '$NAMESPACE'"
  kubectl -n "$NAMESPACE" exec -i "$coord" -- trino --execute "$1" --output-format=CSV
}

smoke_row() {  # smoke_row — the row as CSV, empty when it cannot be read (never fails the script)
  local out=""
  out="$(trino_exec 'SELECT a, msg FROM warehouse.smoke.t' 2>/dev/null)" || out=""
  printf '%s' "$out" | tr -d '"\r' | tail -n 1
}

assert_row() {  # assert_row [attempts] — read the smoke row back, loud on failure
  local attempts="${1:-1}"
  local i got
  for (( i = 1; i <= attempts; i++ )); do
    got="$(smoke_row)"
    if [[ "$got" == "$EXPECTED_ROW" ]]; then
      ok "read back from warehouse.smoke.t: $got"
      return 0
    fi
    if (( i < attempts )); then
      warn "warehouse.smoke.t not readable yet (attempt $i/$attempts): '${got:-<no output>}'"
      sleep 5
    fi
  done
  warn "last Trino attempt, unfiltered:"
  trino_exec 'SELECT a, msg FROM warehouse.smoke.t' || true
  fail "expected '$EXPECTED_ROW' from warehouse.smoke.t, got '${got:-<no output>}'"
}

# ---------------------------------------------------------------------------
# Phases: what the smoke row must *mean*
# ---------------------------------------------------------------------------

# 7. the row is not a Trino internal table: it is a committed Iceberg snapshot,
#    which is metadata the catalog and the object store both had to accept.
phase_iceberg_metadata() {
  local snaps
  # shellcheck disable=SC2016  # "$snapshots" is part of the Iceberg table name, not a shell variable
  snaps="$(trino_exec 'SELECT count(*) FROM warehouse.smoke."t$snapshots"' | tr -d '"\r' | tail -n 1)"
  [[ "$snaps" =~ ^[0-9]+$ ]] || fail "could not read Iceberg metadata table t\$snapshots, got '${snaps:-<no output>}'"
  (( snaps >= 1 )) || fail "Iceberg table warehouse.smoke.t has no snapshots (got $snaps)"
  ok "warehouse.smoke.t has $snaps committed Iceberg snapshot(s)"
}

# 8. durability: restart the object store and read the row again. This is the
#    regression detector for the documented failure mode in the platform values —
#    an emptyDir data dir loses the Parquet files on pod restart while the
#    Iceberg metadata still points at them. Not an HA test: one node, one volume.
phase_object_store_restart() {
  log "restarting statefulset/seaweedfs-volume (its data dir must be a PVC)"
  kubectl -n "$NAMESPACE" rollout restart statefulset/seaweedfs-volume
  wait_seconds "statefulset/seaweedfs-volume after restart" 240 \
    kubectl -n "$NAMESPACE" rollout status statefulset/seaweedfs-volume
  assert_row 12
}

summary() {
  echo
  echo "──────────────────────────────────────────────────────────────"
  echo "✓ verification gate passed — the platform installed on a clean"
  echo "  cluster and round-tripped a row through Trino → Iceberg → S3."
  echo "  (Nova, Airflow, Superset, JupyterHub: out of scope, see README)"
  echo "──────────────────────────────────────────────────────────────"
  local line
  for line in "${PHASE_LOG[@]}"; do printf '  %s\n' "$line"; done
  printf '  total %ss (budget %ss)\n' "$SECONDS" "$GATE_BUDGET_SECONDS"
}

main() {
  log "verification gate: cluster '$CLUSTER', namespace '$NAMESPACE', budget ${GATE_BUDGET_SECONDS}s"
  log "platform installer: ${STARDELT_PLATFORM_DIR:-$REPO_ROOT/../stardelt-platform}"

  phase "install (kind + platform install-core SLIM=1)" phase_install
  phase "scope (exclude Nova)"                          phase_scope
  phase "helm releases"                                 phase_releases
  phase "readiness"                                     phase_ready
  phase "no failing pods"                               phase_no_broken_pods
  phase "smoke (Trino → Iceberg → S3 round-trip)"       phase_smoke
  phase "iceberg metadata (real snapshot)"              phase_iceberg_metadata
  phase "durability (object store restart, re-read)"    phase_object_store_restart

  summary
}

main "$@"
