#!/usr/bin/env bash
# WP-02, integration tier (nightly): install the **full** profile and prove the
# demo actually demos — the NYC-taxi DAG runs, and the rows it loaded are
# readable through Trino.
#
#   kind cluster, with the two stardelt-owned images the full profile runs
#     (airflow from this repo, superset from stardelt-platform, both built and
#     kind-loaded: kind has no registry and CI has no ghcr credentials)
#   → stardelt-platform `make install` (full profile: core stack + Airflow +
#     Superset + Nova)
#   → kind/smoke.sh: the core data path still round-trips in the same run
#   → Airflow: `nyc_taxi_load` is unpaused and triggered; the run must reach
#     `success` — this is the leg's only long pole
#   → Trino: the Iceberg table the DAG wrote is asserted — 12 monthly
#     partitions, a plausible row count, the canonical column set, and one
#     Iceberg snapshot per append.
#
# Scope and exclusions: README.md ("Nightly demo leg") and
# .github/workflows/nightly.yml. Short version — Nova is dropped (CI builds no
# image for it), Superset is installed and asserted only as *ready* (no
# dashboard is exercised; the platform's `install` target includes it, so the
# run cannot ignore it), and ingress/TLS, OIDC and the operator are out of
# scope.
#
# Bounded by construction: every start-up wait is a `kubectl wait`/`rollout
# status` with a timeout. The DAG run is polled against a deadline (it is the
# one thing whose duration is the product), never slept at. The first failure
# exits non-zero; kind/diagnostics.sh (run by the workflow on failure) dumps
# the pods, events, logs and helm history.
#
# Locally (needs docker + kind + kubectl + helm + jq and a platform sibling):
#   bash kind/demo.sh
#   DEMO_BUDGET_SECONDS=5400 bash kind/demo.sh     # slower laptop / node
set -euo pipefail

CLUSTER="${CLUSTER:-stardelt}"
NAMESPACE="${NAMESPACE:-stardelt}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# The full profile (Airflow + Superset). SLIM is deliberately not set: the
# small-node overrides are core-only, and this leg exists to prove the full
# install that the maintainer's 8 GB node cannot run.
export APP_PROFILE=full
# Only the images the full profile runs. `build` would also build Nova from a
# stardelt-nova sibling checkout, which this leg does not test (and CI does not
# have); `up.sh` accepts a list for exactly this case.
export STARDELT_IMAGES="${STARDELT_IMAGES:-airflow superset}"

# Total wall-clock budget for the phases below. Bounded on purpose: a stuck leg
# must fail loudly inside the workflow's job timeout, not hang until the runner
# kills it.
DEMO_BUDGET_SECONDS="${DEMO_BUDGET_SECONDS:-4500}"

# Cap for the DAG run itself (the whole point of the leg). The budget can still
# cut it short — `bounded` takes the smaller of the two.
DAG_WAIT_SECONDS="${DAG_WAIT_SECONDS:-2400}"

DAG_ID="${DAG_ID:-nyc_taxi_load}"
# The DAG writes the Iceberg table `nyc_taxi.yellow_trips` (catalog `warehouse`).
TRINO_TABLE="warehouse.nyc_taxi.yellow_trips"   # quoted for the shell's sake
RUN_ID="nightly-$(date -u +%Y%m%dT%H%M%SZ)"

# The DAG loads every month of 2023 (`YEARS=2023` in helm-values/airflow.yaml):
# 12 files of 47–57 MB, 2.9–3.6M trips each, ~39M for the year. The assertion
# checks bands, not exact counts: TLC republishes a file occasionally, and this
# must catch a broken demo (a month that never landed, an empty table, a schema
# change) without turning a revision into noise. The exact numbers are printed
# as evidence on every green run.
EXPECTED_MONTHS=12
MIN_ROWS_TOTAL=30000000
MAX_ROWS_TOTAL=45000000
MIN_ROWS_MONTH=2000000
MAX_ROWS_MONTH=4000000
EXPECTED_COLUMNS=20

PHASE_LOG=()
DAG_STATE=""
DAG_SECONDS=""
ROWS_TOTAL=""
ROWS_SUM_PARTITIONS=""
SNAPSHOTS=""
COLUMNS=""

log()  { printf '\033[1;34m▶\033[0m [%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }
ok()   { printf '\033[1;32m✓\033[0m [%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }
warn() { printf '\033[1;33m!\033[0m [%s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }
fail() { printf '\033[1;31m✗\033[0m [%s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; exit 1; }

trap 'printf "\033[1;31m✗\033[0m demo leg aborted at line %s: %s\n" "$LINENO" "$BASH_COMMAND" >&2' ERR

DEADLINE=$(( SECONDS + DEMO_BUDGET_SECONDS ))

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
  (( t > 0 )) || fail "budget of ${DEMO_BUDGET_SECONDS}s exhausted before waiting for '$label'"
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
# Cluster helpers
# ---------------------------------------------------------------------------

airflow_exec() {  # airflow_exec <airflow arguments...> — the CLI, in the scheduler
  kubectl -n "$NAMESPACE" exec deploy/airflow-scheduler -c scheduler -- airflow "$@"
}

trino_exec() {  # trino_exec <sql> — run one statement in the Trino coordinator
  local coord
  coord="$(kubectl -n "$NAMESPACE" get pod \
    -l 'app.kubernetes.io/name=trino,app.kubernetes.io/component=coordinator' \
    -o jsonpath='{.items[0].metadata.name}')"
  [[ -n "$coord" ]] || fail "no Trino coordinator pod in namespace '$NAMESPACE'"
  kubectl -n "$NAMESPACE" exec -i "$coord" -- trino --execute "$1" --output-format=CSV
}

# trino_rows <sql> — CSV rows (no header in Trino's CSV format), quotes stripped.
trino_rows() { trino_exec "$1" | tr -d '"\r' | sed '/^$/d'; }
# trino_scalar <sql> — the single value of a one-row/one-column query.
trino_scalar() { trino_rows "$1" | tail -n 1; }

# ---------------------------------------------------------------------------
# Phases
# ---------------------------------------------------------------------------

# 1. cluster + install: kind cluster, the two stardelt images, platform
#    `make install` (full profile). up.sh also applies the platform's own dev
#    S3 credentials manifest, which `make install` expects to exist.
phase_install() {
  bash "$REPO_ROOT/kind/up.sh"
}

# 2. scope: the full profile applies the Nova Deployment, which needs
#    ghcr.io/stardelt/nova plus a ghcr-creds pull secret that CI does not have.
#    Nova is not part of this leg, so drop the Deployment before the
#    broken-pods check. Nothing else the platform installed is touched.
phase_scope() {
  kubectl -n "$NAMESPACE" delete deployment/nova --ignore-not-found --wait=true
  kubectl -n "$NAMESPACE" delete pods -l app.kubernetes.io/name=nova --ignore-not-found --wait=true --timeout=120s
  ok "Nova dropped from the demo leg (no image built here; not part of the DAG path)"
}

# 3. the full install really produced the releases it promises.
phase_releases() {
  local expected=(cnpg seaweedfs lakekeeper trino airflow superset)
  local rel status
  for rel in "${expected[@]}"; do
    status="$(helm -n "$NAMESPACE" list --all --filter "^${rel}\$" -o json | jq -r '.[0].status // "missing"')"
    [[ "$status" == "deployed" ]] || fail "helm release '$rel' is '$status' (expected 'deployed')"
    ok "helm release $rel: deployed"
  done
}

# 4. readiness of everything the DAG path depends on, in dependency order,
#    each wait bounded.
phase_ready() {
  # The one-shot jobs are checked first: they carry ttlSecondsAfterFinished
  # (lakekeeper bootstrap 600s, the Airflow migration/create-user hooks 300s),
  # so waiting for them late in a slow run could race their garbage collection.
  # If they are already gone, the work they did is proven by what follows.
  local job
  for job in lakekeeper-bootstrap airflow-run-airflow-migrations airflow-create-user superset-init-db; do
    if kubectl -n "$NAMESPACE" get "job/$job" >/dev/null 2>&1; then
      wait_seconds "job/$job" 300 kubectl -n "$NAMESPACE" wait --for=condition=complete "job/$job"
    else
      warn "job/$job is gone (ttlSecondsAfterFinished) — what it did is proven by the phases below"
    fi
  done

  # Core data path, the Airflow components (scheduler, DAG processor, api-server,
  # triggerer, statsd, the bundled metadata DB), then Superset and its bundled
  # Postgres + Redis. Every name is asserted to exist first, so a chart bump
  # that renames one fails loudly here instead of waiting for nothing.
  local w
  local workloads=(
    deployment/cnpg-cloudnative-pg
    statefulset/seaweedfs-master
    statefulset/seaweedfs-volume
    statefulset/seaweedfs-filer
    deployment/seaweedfs-s3
    deployment/lakekeeper
    deployment/trino-coordinator
    deployment/trino-worker
    deployment/airflow-api-server
    deployment/airflow-scheduler
    deployment/airflow-dag-processor
    deployment/airflow-triggerer
    deployment/airflow-statsd
    statefulset/airflow-postgresql
    deployment/superset
    deployment/superset-worker
    statefulset/superset-postgresql
    statefulset/superset-redis-master
  )
  for w in "${workloads[@]}"; do
    kubectl -n "$NAMESPACE" get "$w" >/dev/null 2>&1 || fail "expected workload '$w' is not installed by the full profile"
    wait_seconds "$w" 420 kubectl -n "$NAMESPACE" rollout status "$w"
  done

  # CNPG's Cluster condition (Lakekeeper's Postgres); `make install` applies the
  # Cluster but does not wait for it, so re-assert it here.
  wait_seconds "cluster/lakekeeper-pg" 120 \
    kubectl -n "$NAMESPACE" wait --for=condition=Ready cluster/lakekeeper-pg
}

# 5. no workload in the namespace may be crashlooping or unpullable. Catches the
#    "installed but broken" cases the rollouts above cannot see.
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

# 6. the core acceptance script of this repo: write + read through Trino →
#    Iceberg → S3, in the same cluster the DAG is about to write to.
phase_smoke() {
  NAMESPACE="$NAMESPACE" bash "$REPO_ROOT/kind/smoke.sh"
}

# 7. the DAG is parsed and visible to the scheduler (the dag-processor wrote it
#    to the metadata DB). Bounded poll; the scheduler is already Ready.
phase_dag_registered() {
  local cap t
  cap="$(bounded 420)"
  (( cap > 0 )) || fail "budget exhausted before waiting for the DAG to be parsed"
  log "waiting for DAG '$DAG_ID' to be parsed (up to ${cap}s)"
  t=$(( SECONDS + cap ))
  while (( SECONDS < t )); do
    if airflow_exec dags list -o json 2>/dev/null | grep -q "$DAG_ID"; then
      ok "DAG '$DAG_ID' is registered"
      airflow_exec dags unpause "$DAG_ID"
      ok "DAG '$DAG_ID' unpaused"
      return 0
    fi
    sleep 15
  done
  warn "last 'airflow dags list' output:"
  airflow_exec dags list || true
  fail "DAG '$DAG_ID' was not parsed within ${cap}s (dag-processor or DAG import problem)"
}

# 8. trigger the DAG and wait for a terminal state, bounded. On failure the
#    failing run's task states and the task pods' logs are printed here, before
#    the workflow's diagnostics step runs.
phase_dag_run() {
  local cap t state last="" json
  log "triggering DAG '$DAG_ID' (run id '$RUN_ID')"
  json="$(airflow_exec dags trigger "$DAG_ID" --run-id "$RUN_ID" -o json)" || {
    printf '%s\n' "$json" >&2
    fail "could not trigger DAG '$DAG_ID'"
  }
  printf '%s\n' "$json"
  ok "triggered: $RUN_ID"

  cap="$(bounded "$DAG_WAIT_SECONDS")"
  (( cap > 0 )) || fail "budget exhausted before waiting for the DAG run"
  log "waiting for the DAG run to finish (up to ${cap}s)"
  t=$(( SECONDS + cap ))
  local start=$SECONDS
  while (( SECONDS < t )); do
    state="$(airflow_exec dags state "$DAG_ID" "$RUN_ID" 2>/dev/null | tr -d '\r' | tail -n 1)" || state=""
    case "$state" in
      success)
        DAG_STATE="$state"; DAG_SECONDS=$(( SECONDS - start ))
        ok "DAG run $RUN_ID: success after ${DAG_SECONDS}s"
        log "task states:"
        airflow_exec tasks states-for-dag-run "$DAG_ID" "$RUN_ID" -o json || true
        return 0
        ;;
      failed)
        DAG_STATE="$state"; DAG_SECONDS=$(( SECONDS - start ))
        warn "DAG run $RUN_ID: $state after ${DAG_SECONDS}s"
        echo "--- task states"
        airflow_exec tasks states-for-dag-run "$DAG_ID" "$RUN_ID" -o json || true
        echo "--- task pods"
        kubectl -n "$NAMESPACE" get pods -l "dag_id=$DAG_ID" -o wide || true
        echo "--- task pod logs (tail)"
        kubectl -n "$NAMESPACE" logs -l "dag_id=$DAG_ID" --all-containers --prefix --tail=60 || true
        airflow_exec dags list-runs -d "$DAG_ID" -o json || true
        fail "DAG run '$RUN_ID' failed — see the task output above and the diagnostics step of this run"
        ;;
      *)
        if [[ "$state" != "$last" ]]; then
          log "DAG run state: ${state:-<unknown>} ($(( SECONDS - start ))s)"
          last="$state"
        fi
        ;;
    esac
    sleep 20
  done
  warn "last state seen: '${state:-<unknown>}'"
  kubectl -n "$NAMESPACE" logs -l "dag_id=$DAG_ID" --all-containers --prefix --tail=60 || true
  fail "DAG run '$RUN_ID' did not finish within ${cap}s"
}

# 9. the assertion: the table the DAG wrote is real, complete, and readable
#    through Trino — row count, per-month partitions, columns, snapshots.
phase_assert() {
  local tries=0
  while (( tries < 18 )); do
    ROWS_TOTAL="$(trino_scalar "SELECT count(*) FROM $TRINO_TABLE" 2>/dev/null || true)"
    [[ "$ROWS_TOTAL" =~ ^[0-9]+$ ]] && break
    tries=$(( tries + 1 ))
    sleep 10
  done
  [[ "$ROWS_TOTAL" =~ ^[0-9]+$ ]] || {
    warn "last attempt, unfiltered:"
    trino_exec "SELECT count(*) FROM $TRINO_TABLE" || true
    fail "could not read $TRINO_TABLE through Trino (attempts: $tries)"
  }
  ok "Trino sees $TRINO_TABLE: $ROWS_TOTAL rows"
  (( ROWS_TOTAL >= MIN_ROWS_TOTAL && ROWS_TOTAL <= MAX_ROWS_TOTAL )) ||
    fail "row count $ROWS_TOTAL outside the expected ${MIN_ROWS_TOTAL}–${MAX_ROWS_TOTAL} band"

  # Iceberg's own view of the same table: one partition per month, with the
  # record counts it committed. Also a cross-check on the Trino count above.
  # shellcheck disable=SC2016  # "$partitions" is part of the Iceberg table name
  local part_sql='SELECT partition.pickup_year_month, record_count FROM warehouse.nyc_taxi."yellow_trips$partitions" ORDER BY 1'
  # shellcheck disable=SC2016  # "$snapshots" is part of the Iceberg table name
  local snap_sql='SELECT count(*) FROM warehouse.nyc_taxi."yellow_trips$snapshots"'
  local parts
  parts="$(trino_rows "$part_sql")"
  printf '%s\n' "$parts"
  local months sum
  months="$(printf '%s\n' "$parts" | sed '/^$/d' | wc -l | tr -d ' ')"
  (( months == EXPECTED_MONTHS )) ||
    fail "expected $EXPECTED_MONTHS monthly partitions, Iceberg reports $months"
  sum="$(printf '%s\n' "$parts" | awk -F, '{ s += $2 } END { printf "%.0f", s }')"
  ROWS_SUM_PARTITIONS="$sum"
  (( sum == ROWS_TOTAL )) ||
    fail "Iceberg partition records sum to $sum but Trino counts $ROWS_TOTAL rows"
  local line ym rows
  while IFS= read -r line; do
    [[ -n "$line" ]] || continue
    ym="${line%%,*}"
    rows="${line##*,}"
    [[ "$ym" =~ ^[0-9]{6}$ ]] || fail "unexpected partition key '$ym' in $TRINO_TABLE\$partitions"
    [[ "$rows" =~ ^[0-9]+$ ]] || fail "unexpected record count '$rows' for month $ym"
    (( rows >= MIN_ROWS_MONTH && rows <= MAX_ROWS_MONTH )) ||
      fail "month $ym has $rows rows, outside the expected ${MIN_ROWS_MONTH}–${MAX_ROWS_MONTH} band"
  done <<< "$parts"
  ok "12 monthly partitions, each within ${MIN_ROWS_MONTH}–${MAX_ROWS_MONTH} rows (sum $sum)"

  SNAPSHOTS="$(trino_scalar "$snap_sql")"
  [[ "$SNAPSHOTS" =~ ^[0-9]+$ ]] || fail "could not read $TRINO_TABLE\$snapshots, got '${SNAPSHOTS:-<no output>}'"
  (( SNAPSHOTS >= EXPECTED_MONTHS )) ||
    fail "expected at least $EXPECTED_MONTHS Iceberg snapshots (one per append), got $SNAPSHOTS"
  ok "Iceberg metadata: $SNAPSHOTS committed snapshots"

  local cols
  cols="$(trino_rows "SELECT column_name, data_type FROM warehouse.information_schema.columns WHERE table_schema = 'nyc_taxi' AND table_name = 'yellow_trips' ORDER BY ordinal_position")"
  COLUMNS="$(printf '%s\n' "$cols" | sed '/^$/d' | wc -l | tr -d ' ')"
  (( COLUMNS == EXPECTED_COLUMNS )) ||
    fail "expected $EXPECTED_COLUMNS columns in $TRINO_TABLE, found $COLUMNS"
  local required=(pickup_year_month tpep_pickup_datetime tpep_dropoff_datetime total_amount fare_amount pu_location_id)
  local c
  for c in "${required[@]}"; do
    printf '%s\n' "$cols" | grep -q "^${c}," ||
      fail "column '$c' is missing from $TRINO_TABLE (columns: $(printf '%s' "$cols" | tr '\n' ' '))"
  done
  ok "column set: $COLUMNS columns, all canonical names present"
}

summary() {
  echo
  echo "──────────────────────────────────────────────────────────────"
  echo "✓ nightly demo leg passed — the full profile installed, the"
  echo "  NYC-taxi DAG ran, and its rows read back through Trino."
  echo
  echo "  DAG run   : $DAG_ID / $RUN_ID — $DAG_STATE in ${DAG_SECONDS}s"
  echo "  Trino     : $TRINO_TABLE — $ROWS_TOTAL rows in $EXPECTED_MONTHS monthly partitions"
  echo "  Iceberg   : $SNAPSHOTS snapshots, partition metadata sums to $ROWS_SUM_PARTITIONS rows"
  echo "  columns   : $COLUMNS"
  echo "  (Nova dropped, Superset readiness only — see README.md)"
  echo "──────────────────────────────────────────────────────────────"
  local line
  for line in "${PHASE_LOG[@]}"; do printf '  %s\n' "$line"; done
  printf '  total %ss (budget %ss)\n' "$SECONDS" "$DEMO_BUDGET_SECONDS"
}

main() {
  log "nightly demo leg: cluster '$CLUSTER', namespace '$NAMESPACE', budget ${DEMO_BUDGET_SECONDS}s"
  log "platform installer: ${STARDELT_PLATFORM_DIR:-$REPO_ROOT/../stardelt-platform}"
  log "DAG run id: $RUN_ID"

  phase "install (kind + platform full profile)"           phase_install
  phase "scope (drop Nova)"                                phase_scope
  phase "helm releases"                                    phase_releases
  phase "readiness (core + Airflow + Superset)"            phase_ready
  phase "no failing pods"                                  phase_no_broken_pods
  phase "smoke (Trino → Iceberg → S3 round-trip)"          phase_smoke
  phase "DAG registered ($DAG_ID parsed and unpaused)"     phase_dag_registered
  phase "DAG run (trigger + bounded wait)"                 phase_dag_run
  phase "assert (rows, partitions, columns via Trino)"     phase_assert

  summary
}

main "$@"
