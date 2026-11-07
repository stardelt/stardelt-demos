# PEGELONLINE — water levels

Source: [PEGELONLINE](https://www.pegelonline.wsv.de/) (German federal waterway gauges), public JSON API with no key. 787 stations, a new measurement every 15 minutes. Hourly ingestion shows every value at most 60 minutes late.

Tables:
- `shared.bronze.pegelonline_measurements` (appended by Airflow DAG `pegelonline_hourly`, scheduled `7 * * * *`)
- `shared.silver.pegelonline_measurements` (deduplicated by `(station_uuid, timeseries, measured_at)`, keep newest `ingested_at`)
- `shared.gold.pegelonline_station_latest` (latest measurement per `(station_uuid, timeseries)`)
- `shared.gold.pegelonline_station_hourly` (hourly averages/min/max per gauge/timeseries)

## Setup

```bash
# 1. The Keycloak client and the Secret sp-pipeline-stardelt-users
make sync-principal NAME=pipeline-stardelt-users GROUP=stardelt-users AUDIENCES="trino lakekeeper"
# 2. The resource the pipeline loader checks (make sync-principal does NOT create it)
kubectl apply -f - <<'YAML'
apiVersion: stardelt.io/v1alpha1
kind: ServicePrincipal
metadata: { name: pipeline-stardelt-users, namespace: stardelt }
spec: { ownerGroup: stardelt-users, purpose: pipeline, audiences: [trino, lakekeeper] }
YAML
kubectl -n stardelt wait --for=condition=Ready sp/pipeline-stardelt-users --timeout=120s
# 3. Wait for access-sync (CronJob */2) to give the principal the group's Lakekeeper/Trino roles,
#    and for airflow-group-roles (CronJob */5) to create the Airflow role the DAG needs.
kubectl apply -f examples/pegelonline/pipeline.yaml
# 4. Within ~5 minutes: `kubectl -n stardelt exec deploy/airflow-scheduler -c scheduler -- airflow dags list | grep pegelonline`
```

`shared` is the platform's shared catalog (owner `stardelt-admins`, `browse/select/write/create` for `stardelt-users`).

The **bronze write** runs in the Airflow task pod as `svc-catalog-airflow` (the deployment carries `CATALOG_CLIENT_ID`/`CATALOG_CLIENT_SECRET`/`CATALOG_TOKEN_URL`). Verify it is a member of `stardelt-users` before the first run:

```bash
kubectl -n stardelt logs job/$(kubectl -n stardelt get jobs -o name | grep access-sync | tail -1) \
  | grep 'role stardelt-users'
# expect: "role stardelt-users: 8 assignee(s) (7 member(s), 1 static), …"
```

If the first DAG run ends in `Forbidden`/`NotAuthorized` from Lakekeeper, stop and comment on this ticket with the log — the fix is a grant for the Airflow client's group, not a change here.

## Freshness queries

```sql
SELECT max(measured_at) AS newest, max(ingested_at) AS ingested FROM shared.bronze.pegelonline_measurements;
SELECT timeseries, count(*) FROM shared.silver.pegelonline_measurements GROUP BY 1 ORDER BY 2 DESC;
SELECT * FROM shared.gold.pegelonline_station_latest WHERE timeseries = 'W' ORDER BY measurement_value DESC LIMIT 10;
SELECT * FROM shared.gold.pegelonline_station_hourly WHERE timeseries = 'W' ORDER BY measured_hour DESC LIMIT 10;
```

The DAG also publishes to `pegelonline.measurements` when `PEGELONLINE_KAFKA_BOOTSTRAP` is set (e.g. `kafka.stardelt.svc.cluster.local:9092`), one JSON message per (station, timeseries), key `station_uuid:timeseries`; unset, the DAG only appends to Iceberg.
