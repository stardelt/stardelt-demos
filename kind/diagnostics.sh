#!/usr/bin/env bash
# Dump everything needed to explain a red verification run.
#
# Called by .github/workflows/verify.yml on failure (`if: failure()`), and by
# hand after a failed kind/gate.sh:
#
#   bash kind/diagnostics.sh
#
# Best-effort by design: it never fails and never hangs (every command is
# wrapped in `timeout`, every log tail is capped), so it cannot mask the real
# failure of the run it describes, and it cannot stall the job. Output goes to
# stdout — it is meant to be read in the workflow log:
#
#   gh run view --log-failed
set -uo pipefail

NAMESPACE="${NAMESPACE:-stardelt}"
CLUSTER="${CLUSTER:-stardelt}"
NODE_CONTAINER="${CLUSTER}-control-plane"
CMD_TIMEOUT="${DIAG_CMD_TIMEOUT:-60}"
LOG_TAIL="${DIAG_LOG_TAIL:-120}"

# kt <kubectl args...> — bounded kubectl, output merged, never fatal.
kt() { timeout "$CMD_TIMEOUT" kubectl "$@" 2>&1 || echo "(kubectl: no output / failed)"; }
# ktq <kubectl args...> — same, but silent; for use inside $( ) loops.
ktq() { timeout "$CMD_TIMEOUT" kubectl "$@" 2>/dev/null || true; }

section() {
  printf '\n\n\033[1;36m===== %s =====\033[0m\n' "$*"
}

section "context"
{
  date -u
  echo "namespace=${NAMESPACE} cluster=${CLUSTER}"
  timeout 20 kubectl config current-context || true
  command -v kind >/dev/null && timeout 30 kind version
  if command -v kubectl >/dev/null; then timeout 30 kubectl version; fi
  command -v helm >/dev/null && timeout 30 helm version --short
  echo
  free -m || true
  echo
  df -h / /var/lib/docker /tmp 2>/dev/null || true
}

section "kind cluster"
{
  timeout 30 kind get clusters || true
  timeout 30 docker ps --filter "name=${CLUSTER}" --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' || true
}

section "kind node internals (containerd, disk)"
{
  timeout "$CMD_TIMEOUT" docker exec "$NODE_CONTAINER" crictl ps -a 2>&1 | tail -n 30 || true
  echo "--- images present in the node"
  timeout "$CMD_TIMEOUT" docker exec "$NODE_CONTAINER" crictl images 2>&1 | head -n 20 || true
  echo "--- node disk"
  timeout "$CMD_TIMEOUT" docker exec "$NODE_CONTAINER" df -h / 2>&1 || true
}

section "nodes"
{
  kt get nodes -o wide
  kt describe nodes | grep -vE '^\s*$' | tail -n 40
}

section "namespace workloads"
{
  kt -n "$NAMESPACE" get all,pvc,serviceaccount -o wide
  kt -n "$NAMESPACE" get pods -o custom-columns='POD:.metadata.name,PHASE:.status.phase,RESTARTS:.status.containerStatuses[*].restartCount,NODE:.spec.nodeName,START:.status.startTime'
}

section "recent events (oldest → newest)"
kt -n "$NAMESPACE" get events --sort-by=.lastTimestamp | tail -n 60

section "helm releases"
{
  timeout 60 helm -n "$NAMESPACE" list --all || echo "(helm list failed)"
  for rel in cnpg seaweedfs lakekeeper trino airflow superset; do
    echo "--- helm status $rel"
    timeout 60 helm -n "$NAMESPACE" status "$rel" 2>&1 | head -n 20 || true
  done
  echo "--- helm history (failed/stuck releases)"
  for rel in cnpg seaweedfs lakekeeper trino airflow superset; do
    timeout 60 helm -n "$NAMESPACE" history "$rel" 2>&1 | tail -n 5 || true
  done
}

section "describe: pods that are not Running/Completed"
for pod in $(ktq -n "$NAMESPACE" get pods -o jsonpath='{.items[?(@.status.phase!="Running")].metadata.name}'); do
  echo "--- describe pod/$pod"
  kt -n "$NAMESPACE" describe pod "$pod" | grep -vE '^\s*$' | tail -n 70
done

section "describe: Running pods that are not Ready"
for pod in $(ktq -n "$NAMESPACE" get pods -o jsonpath='{.items[?(@.status.phase=="Running")].metadata.name}'); do
  ready="$(ktq -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')"
  if [[ "$ready" != "True" ]]; then
    echo "--- describe pod/$pod (Ready=$ready)"
    kt -n "$NAMESPACE" describe pod "$pod" | grep -vE '^\s*$' | tail -n 70
  fi
done

section "logs (tail ${LOG_TAIL} per container, previous when restarted)"
for pod in $(ktq -n "$NAMESPACE" get pods -o jsonpath='{.items[?(@.status.phase!="Succeeded")].metadata.name}'); do
  for ctr in $(ktq -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.spec.containers[*].name}'); do
    echo "--- logs pod/$pod container/$ctr"
    kt -n "$NAMESPACE" logs "$pod" -c "$ctr" --tail="$LOG_TAIL" --prefix=false
    restarts="$(ktq -n "$NAMESPACE" get pod "$pod" -o jsonpath="{.status.containerStatuses[?(@.name=='$ctr')].restartCount}")"
    if [[ "${restarts:-0}" -gt 0 ]]; then
      echo "--- logs pod/$pod container/$ctr (previous instance)"
      kt -n "$NAMESPACE" logs "$pod" -c "$ctr" --previous --tail="$LOG_TAIL"
    fi
  done
done

section "airflow (full profile / nightly demo leg)"
{
  echo "--- helm status airflow"
  timeout 60 helm -n "$NAMESPACE" status airflow 2>&1 | head -n 20 || echo "(no airflow release)"
  echo "--- deployments, statefulsets, jobs"
  kt -n "$NAMESPACE" get deploy,statefulset,job -l tier=airflow -o wide
  echo "--- pods (incl. KubernetesExecutor task pods)"
  kt -n "$NAMESPACE" get pods -l tier=airflow -o wide
  echo "--- DAG runs"
  kt -n "$NAMESPACE" exec deploy/airflow-scheduler -c scheduler -- airflow dags list-runs -d nyc_taxi_load -o json 2>&1 | tail -n 40
  echo "--- DAG import errors (empty = all DAGs parsed)"
  kt -n "$NAMESPACE" exec deploy/airflow-scheduler -c scheduler -- airflow dags list-import-errors 2>&1 | tail -n 30
  echo "--- task pod logs (latest per pod, tail ${LOG_TAIL})"
  for pod in $(ktq -n "$NAMESPACE" get pods -l tier=airflow,component=worker -o jsonpath='{.items[*].metadata.name}'); do
    echo "--- logs pod/$pod"
    kt -n "$NAMESPACE" logs "$pod" --all-containers --tail="$LOG_TAIL" --prefix=false
  done
}

section "superset (full profile; installed, readiness-only in the demo leg)"
{
  echo "--- helm status superset"
  timeout 60 helm -n "$NAMESPACE" status superset 2>&1 | head -n 20 || echo "(no superset release)"
  kt -n "$NAMESPACE" get deploy,statefulset,job -l app=superset -o wide
  kt -n "$NAMESPACE" get pods -l app=superset -o wide
  echo "--- logs (tail 40 each)"
  for pod in $(ktq -n "$NAMESPACE" get pods -l app=superset -o jsonpath='{.items[*].metadata.name}'); do
    echo "--- logs pod/$pod"
    kt -n "$NAMESPACE" logs "$pod" --all-containers --tail=40 --prefix=false
  done
}

section "trino: catalog config, coordinator health, last query failure"
{
  kt -n "$NAMESPACE" exec deploy/trino-coordinator -c trino-coordinator -- cat /etc/trino/catalog/warehouse.properties
  kt -n "$NAMESPACE" exec deploy/trino-coordinator -c trino-coordinator -- curl -sS -m 10 localhost:8080/v1/info
  echo
  kt -n "$NAMESPACE" logs deploy/trino-coordinator -c trino-coordinator --tail=60 | grep -iE 'error|exception|fail' | tail -n 30
}

section "lakekeeper + object store (best effort)"
{
  echo "--- lakekeeper /management/v1/info (via the bootstrap image, which has curl)"
  kt -n "$NAMESPACE" logs job/lakekeeper-bootstrap --tail=40
  echo "--- warehouse response captured by the bootstrap job is above"
  echo "--- seaweedfs volume data dir"
  kt -n "$NAMESPACE" exec statefulset/seaweedfs-volume -c seaweedfs -- sh -c 'ls -la /data; du -sh /data/* 2>/dev/null | head -n 10' 2>&1 | head -n 30
}

section "kubernetes objects, full (for paste-into-issue)"
{
  kt -n "$NAMESPACE" get pods,deploy,statefulset,job,pvc -o wide
  echo
  kt -n "$NAMESPACE" get cluster.postgresql.cnpg.io/lakekeeper-pg -o yaml | head -n 60
}

section "what to look at next"
cat <<'EOF'
- A component that never became Ready: search the section above for its
  `describe` and `logs` blocks. `Events:` at the bottom of a describe names the
  reason (image pull, volume, probe, OOM).
- Trino problems: the catalog config and /v1/info are printed above; the
  coordinator log lines filtered for errors follow it.
- Airflow / the NYC-taxi DAG (nightly demo leg): the "airflow" section has the
  DAG runs, the DAG import errors and every KubernetesExecutor task pod's log.
  A task pod that is gone was deleted after success (delete_worker_pods); a
  failed one is kept. `airflow tasks states-for-dag-run nyc_taxi_load <run_id>`
  names the failing task, and its pod log says why.
- Superset (installed by the full profile, readiness-only in the demo leg): its
  init job and pods are in the "superset" section.
- Nothing installed at all: check the "helm releases" section — a failing
  `helm upgrade --install` is reported there with its history.
- Reproduce locally with the same scope as the per-PR gate:
    STARDELT_IMAGES=none APP_PROFILE=core SLIM=1 bash kind/up.sh && bash kind/smoke.sh
  the whole gate: bash kind/gate.sh
  or the nightly demo leg: bash kind/demo.sh
EOF

exit 0
