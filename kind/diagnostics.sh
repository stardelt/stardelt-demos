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
  for rel in cnpg seaweedfs lakekeeper trino; do
    echo "--- helm status $rel"
    timeout 60 helm -n "$NAMESPACE" status "$rel" 2>&1 | head -n 20 || true
  done
  echo "--- helm history (failed/stuck releases)"
  for rel in cnpg seaweedfs lakekeeper trino; do
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
- Nothing installed at all: check the "helm releases" section — a failing
  `helm upgrade --install` is reported there with its history.
- Reproduce locally with the same scope as CI:
    STARDELT_IMAGES=none APP_PROFILE=core SLIM=1 bash kind/up.sh && bash kind/smoke.sh
  or run the whole gate: bash kind/gate.sh
EOF

exit 0
