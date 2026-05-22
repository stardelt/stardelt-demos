"""NYC TLC yellow-taxi → Iceberg via Lakekeeper.

One Airflow task per (year, month). Dynamic mapping fans out across the
configured year range; Iceberg appends are intrinsically serial per table
(REST catalog optimistic concurrency), so a sensible `max_active_tasks`
keeps parallelism downloading while commits queue.

Idempotency: a task whose (year, month) partition is already present in the
table short-circuits.

Config via environment variables on the worker pod, sourced from K8s
Secret `stardelt-s3-creds` and ConfigMap `stardelt-runtime`:
  CATALOG_URI, CATALOG_WAREHOUSE, S3_ENDPOINT, S3_ACCESS_KEY,
  S3_SECRET_KEY, S3_REGION, YEARS.
"""
from __future__ import annotations

import io
import os
from datetime import datetime

from airflow.sdk import dag, task

TLC_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet"
NAMESPACE = "nyc_taxi"
TABLE = "yellow_trips"

# Canonical schema columns (snake_case). Defined inside _build_table to
# avoid importing pyiceberg at DAG-parse time on the scheduler.
RENAME_MAP = {
    "VendorID": "vendor_id",
    "RatecodeID": "ratecode_id",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "Airport_fee": "airport_fee",
    "airport_fee": "airport_fee",
}


def _arrow_schema():
    import pyarrow as pa
    return pa.schema([
        pa.field("vendor_id",             pa.int32()),
        pa.field("tpep_pickup_datetime",  pa.timestamp("us")),
        pa.field("tpep_dropoff_datetime", pa.timestamp("us")),
        pa.field("passenger_count",       pa.float64()),
        pa.field("trip_distance",         pa.float64()),
        pa.field("ratecode_id",           pa.float64()),
        pa.field("store_and_fwd_flag",    pa.string()),
        pa.field("pu_location_id",        pa.int32()),
        pa.field("do_location_id",        pa.int32()),
        pa.field("payment_type",          pa.int32()),
        pa.field("fare_amount",           pa.float64()),
        pa.field("extra",                 pa.float64()),
        pa.field("mta_tax",               pa.float64()),
        pa.field("tip_amount",            pa.float64()),
        pa.field("tolls_amount",          pa.float64()),
        pa.field("improvement_surcharge", pa.float64()),
        pa.field("total_amount",          pa.float64()),
        pa.field("congestion_surcharge",  pa.float64()),
        pa.field("airport_fee",           pa.float64()),
        pa.field("pickup_year_month",     pa.int32()),
    ])


def _catalog():
    from pyiceberg.catalog.rest import RestCatalog
    return RestCatalog(
        name="warehouse",
        **{
            "uri": os.environ["CATALOG_URI"],
            "warehouse": os.environ.get("CATALOG_WAREHOUSE", "warehouse"),
            "s3.endpoint": os.environ["S3_ENDPOINT"],
            "s3.access-key-id": os.environ["S3_ACCESS_KEY"],
            "s3.secret-access-key": os.environ["S3_SECRET_KEY"],
            "s3.region": os.environ.get("S3_REGION", "us-east-1"),
            "s3.path-style-access": "true",
        },
    )


def _ensure_table():
    from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import DoubleType, IntegerType, NestedField, StringType, TimestampType

    catalog = _catalog()
    try:
        catalog.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass

    try:
        return catalog.load_table((NAMESPACE, TABLE))
    except NoSuchTableError:
        schema = Schema(
            NestedField(1,  "vendor_id",             IntegerType()),
            NestedField(2,  "tpep_pickup_datetime",  TimestampType()),
            NestedField(3,  "tpep_dropoff_datetime", TimestampType()),
            NestedField(4,  "passenger_count",       DoubleType()),
            NestedField(5,  "trip_distance",         DoubleType()),
            NestedField(6,  "ratecode_id",           DoubleType()),
            NestedField(7,  "store_and_fwd_flag",    StringType()),
            NestedField(8,  "pu_location_id",        IntegerType()),
            NestedField(9,  "do_location_id",        IntegerType()),
            NestedField(10, "payment_type",          IntegerType()),
            NestedField(11, "fare_amount",           DoubleType()),
            NestedField(12, "extra",                 DoubleType()),
            NestedField(13, "mta_tax",               DoubleType()),
            NestedField(14, "tip_amount",            DoubleType()),
            NestedField(15, "tolls_amount",          DoubleType()),
            NestedField(16, "improvement_surcharge", DoubleType()),
            NestedField(17, "total_amount",          DoubleType()),
            NestedField(18, "congestion_surcharge",  DoubleType()),
            NestedField(19, "airport_fee",           DoubleType()),
            NestedField(20, "pickup_year_month",     IntegerType()),
        )
        partition_spec = PartitionSpec(
            PartitionField(source_id=20, field_id=1000,
                           transform=IdentityTransform(),
                           name="pickup_year_month")
        )
        return catalog.create_table(
            identifier=(NAMESPACE, TABLE),
            schema=schema,
            partition_spec=partition_spec,
        )


def _already_loaded(table, ym: int) -> bool:
    try:
        rows = table.inspect.partitions().to_pylist()
    except Exception:
        return False
    for r in rows:
        part = r.get("partition") or {}
        if part.get("pickup_year_month") == ym and r.get("record_count", 0) > 0:
            return True
    return False


def _years_from_env() -> list[int]:
    raw = os.environ.get("YEARS", "2023")
    if "-" in raw:
        a, b = raw.split("-", 1)
        return list(range(int(a), int(b) + 1))
    return [int(raw)]


@dag(
    dag_id="nyc_taxi_load",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # Iceberg REST catalog uses optimistic concurrency: parallel appends to
    # the same table conflict and roll back. Serializing tasks (=1) is the
    # cheapest fix; commits are <1s so total runtime is dominated by downloads.
    max_active_tasks=1,
    default_args={
        "retries": 3,
        "retry_delay": 10,  # seconds; healing transient REST commit conflicts
    },
    tags=["stardelt", "nyc-taxi"],
)
def nyc_taxi_load():
    @task
    def plan() -> list[dict]:
        years = _years_from_env()
        return [
            {"year": y, "month": m, "ym": y * 100 + m}
            for y in years for m in range(1, 13)
        ]

    @task
    def fetch_and_append(year: int, month: int, ym: int) -> dict:
        import httpx
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = _ensure_table()
        if _already_loaded(table, ym):
            print(f"{year}-{month:02d}: already loaded, skipping")
            return {"ym": ym, "rows": 0, "status": "skipped"}

        url = TLC_URL.format(year=year, month=month)
        print(f"GET {url}")
        with httpx.Client(timeout=120, follow_redirects=True) as client:
            r = client.get(url)
            if r.status_code == 404:
                print(f"{year}-{month:02d}: not yet published")
                return {"ym": ym, "rows": 0, "status": "missing"}
            r.raise_for_status()
            data = r.content
        print(f"  downloaded {len(data) / 1024 / 1024:.1f} MiB")

        raw = pq.read_table(io.BufferedReader(io.BytesIO(data)))
        renames = {old: new for old, new in RENAME_MAP.items() if old in raw.column_names}
        if renames:
            raw = raw.rename_columns([renames.get(n, n) for n in raw.column_names])

        canon = _arrow_schema()
        arrays = []
        for f in canon:
            if f.name == "pickup_year_month":
                arrays.append(pa.array([ym] * raw.num_rows, type=pa.int32()))
            elif f.name in raw.column_names:
                arrays.append(raw.column(f.name).cast(f.type, safe=False))
            else:
                arrays.append(pa.nulls(raw.num_rows, type=f.type))
        arrow = pa.Table.from_arrays(arrays, schema=canon)
        print(f"  appending {arrow.num_rows:,} rows for {year}-{month:02d}")
        table.append(arrow)
        return {"ym": ym, "rows": arrow.num_rows, "status": "loaded"}

    targets = plan()
    fetch_and_append.expand_kwargs(targets)


nyc_taxi_load()
