# Continuous streaming demo — PEGELONLINE-shaped

The hourly `pegelonline_hourly` DAG is a **source connector** for a source that only changes
hourly. This demo exercises the streaming plane itself: a small producer pod writes three
measurements every five seconds to Kafka, and `StreamApp/pegelonline-continuous` reads the topic
continuously and MERGEs into `mobility.bronze.pegelonline_continuous`.

```text
pegelonline-continuous-producer (Deployment, 3 rows / 5 s)
  -> Kafka pegelonline.continuous
  -> StreamApp pegelonline-continuous (Spark Structured Streaming, always on)
  -> mobility.bronze.pegelonline_continuous (Iceberg)
```

This is a demo workload, not part of the platform. The hourly DAG (`dags/pegelonline_hourly.py`)
remains the evidence for `stardelt-demos#52`; this example is the low-latency counterpart.

## Apply order

1. **Create the sink table** with `sink.sql` in the Nova SQL editor as a member of
   `mobility-analytics` (the group owns the `mobility` catalog), or with a principal that can
   create in `mobility.bronze`. The StreamApp does **not** create its sink table.
2. **Create the producer ConfigMap and Deployment**:

   ```bash
   kubectl apply -f producer-configmap.yaml
   kubectl apply -f continuous-demo.yaml
   ```

   `continuous-demo.yaml` carries the `KafkaTopic`, the producer `Deployment` and the `StreamApp`.
   If you want a deterministic start, apply the `KafkaTopic` first and wait for
   `kubectl -n stardelt get kafkatopic pegelonline-continuous` to be `Ready`, then apply the rest.
3. **Verify the stream**:

   ```bash
   kubectl -n stardelt get streamapp pegelonline-continuous
   kubectl -n stardelt logs deploy/pegelonline-continuous-producer --tail=5
   kubectl -n stardelt exec kafka-kafka-0 -- \
     bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 \
     --topic pegelonline.continuous --time -1
   ```

   In Trino (SQL editor as `mobility.analyst`):

   ```sql
   SELECT count(*), max(measured_at) FROM mobility.bronze.pegelonline_continuous;
   ```

   Run it twice, 15 seconds apart; the count grows without any DAG run.

## Live evidence (2026-11-07)

- Producer: `3 rows every 5s`; topic offset grew continuously.
- Sink: **282 → 291 rows in 15 seconds**; `max(measured_at)` **13:41:12 → 13:41:27 UTC**.
- Streams page as `mobility.analyst`: `pegelonline-continuous · Running`; retail sees
  `No streams yet`.
- The hourly `pegelonline` stream was stopped for the 2026-11-07 demo because the old
  full-core Spark requests did not fit; the half-core requests now let both streams share
  the 8 vCPU node (both were `Running` on 2026-11-08). If you need the capacity back, stop
  one stream — the checkpoint is kept:

  ```bash
  kubectl -n stardelt patch streamapp pegelonline --type merge -p '{"spec":{"stopped":true}}'
  ```

## `merge` and `append`

Both sink modes work. `merge` keeps a current-state table: rows are merged on `mergeKeys`,
and when one micro-batch carries two rows for the same key the row with the newest Kafka
offset wins. `append` adds every row and is at-least-once. The sink columns are
`timestamp with time zone` so the ISO-8601 `Z` producer form parses directly.

## Self-service gap (for the architect)

Today only a cluster administrator can create the `StreamApp` and `KafkaTopic`; there is no Nova
create API/UI (`GET /api/streams` lists, `PATCH` stops/resumes). The sink table is created
manually. The Streams page shows phase, source, sink and last error only — no lag, rows,
throughput, checkpoint or history. A product group can observe its own streams but cannot create
one without an administrator. The architecture ticket covers the group-owned creation, sink-table
ownership, observability and capacity story.
