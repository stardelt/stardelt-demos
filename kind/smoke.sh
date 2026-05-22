#!/usr/bin/env bash
# Stage 1 acceptance: round-trip a write+read through Trino → Iceberg → SeaweedFS.
set -euo pipefail

NAMESPACE="${NAMESPACE:-stardelt}"
RELEASE="${TRINO_RELEASE:-trino}"

if ! kubectl -n "$NAMESPACE" get pods -l "app.kubernetes.io/name=trino,app.kubernetes.io/component=coordinator" \
     --field-selector=status.phase=Running 2>/dev/null | grep -q Running; then
  echo "Trino coordinator not running in namespace $NAMESPACE." >&2
  echo "Run \`make up\` first." >&2
  exit 1
fi

COORD=$(kubectl -n "$NAMESPACE" get pod -l "app.kubernetes.io/name=trino,app.kubernetes.io/component=coordinator" \
        -o jsonpath='{.items[0].metadata.name}')

run() {
  kubectl -n "$NAMESPACE" exec -i "$COORD" -- trino --execute "$1" --output-format=CSV
}

echo "▶ creating schema warehouse.smoke"
run "CREATE SCHEMA IF NOT EXISTS warehouse.smoke"
echo "▶ creating table warehouse.smoke.t"
run "DROP TABLE IF EXISTS warehouse.smoke.t"
run "CREATE TABLE warehouse.smoke.t (a INTEGER, msg VARCHAR)"
echo "▶ inserting row"
run "INSERT INTO warehouse.smoke.t VALUES (1, 'hello-iceberg-on-s3')"
echo "▶ reading back"
got=$(run "SELECT a, msg FROM warehouse.smoke.t" | tr -d '"')
expected="1,hello-iceberg-on-s3"
if [[ "$got" == *"$expected"* ]]; then
  echo "✓ Stage 1 smoke passed: $got"
else
  echo "✗ Stage 1 smoke failed. Got: $got" >&2
  exit 1
fi
