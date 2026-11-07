"""PEGELONLINE hourly ingest → Iceberg (shared.bronze)."""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

from airflow.sdk import dag, task

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from stardelt_pipelines.catalog_properties import catalog_properties  # noqa: E402
from stardelt_pipelines.pegelonline import STATIONS_URL, arrow_table, kafka_record, rows_from  # noqa: E402

NAMESPACE = "bronze"  # a schema inside the shared catalog
TABLE = "pegelonline_measurements"
CATALOG = "shared"  # the Lakekeeper warehouse, not the platform's `warehouse`
TOPIC = "pegelonline.measurements"  # D42 §4/§7: the same rows the poller appends


def _catalog():
    from pyiceberg.catalog.rest import RestCatalog

    properties = catalog_properties(os.environ)
    # The NYC one plus one override: CATALOG_WAREHOUSE is `warehouse` on the Airflow pod
    properties["warehouse"] = CATALOG  # write into the shared catalog
    return RestCatalog(name=CATALOG, **properties)


def _ensure_table():
    from pyiceberg.exceptions import (
        NamespaceAlreadyExistsError,
        NoSuchTableError,
        TableAlreadyExistsError,
    )
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import DayTransform
    from pyiceberg.types import DoubleType, NestedField, StringType, TimestampType

    catalog = _catalog()
    try:
        catalog.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass

    try:
        return catalog.load_table((NAMESPACE, TABLE))
    except NoSuchTableError:
        schema = Schema(
            NestedField(1, "station_uuid", StringType(), required=True),
            NestedField(2, "station_number", StringType(), required=True),
            NestedField(3, "station_name", StringType(), required=True),
            NestedField(4, "water", StringType(), required=True),
            NestedField(5, "agency", StringType(), required=True),
            NestedField(6, "km", DoubleType(), required=False),
            NestedField(7, "latitude", DoubleType(), required=False),
            NestedField(8, "longitude", DoubleType(), required=False),
            NestedField(9, "timeseries", StringType(), required=True),
            NestedField(10, "unit", StringType(), required=True),
            NestedField(11, "measured_at", TimestampType(), required=True),
            NestedField(12, "measurement_value", DoubleType(), required=True),
            NestedField(13, "state_mnw_mhw", StringType(), required=False),
            NestedField(14, "state_nsw_hsw", StringType(), required=False),
            NestedField(15, "ingested_at", TimestampType(), required=True),
        )
        partition_spec = PartitionSpec(
            PartitionField(
                source_id=11,
                field_id=1000,
                transform=DayTransform(),
                name="measured_at_day",
            )
        )
        return catalog.create_table(
            identifier=(NAMESPACE, TABLE),
            schema=schema,
            partition_spec=partition_spec,
        )
    except TableAlreadyExistsError:
        # Two runs can reach this point together (a manual and the scheduled one); the loser
        # gets 409 and must read the table the winner created instead of failing (demos#48).
        return catalog.load_table((NAMESPACE, TABLE))


def publish_rows(rows: list[dict], ingested_at: datetime) -> int:
    """Publish every row to the bus when PEGELONLINE_KAFKA_BOOTSTRAP is set; 0 otherwise.

    Unset means the bus is not installed: phase 1 keeps running exactly as before. The producer
    is created per run and closed before the task returns, so no connection outlives the pod.
    """
    bootstrap = os.environ.get("PEGELONLINE_KAFKA_BOOTSTRAP", "").strip()
    if not bootstrap:
        return 0
    from kafka import KafkaProducer  # imported here: the DAG module must load without the client

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        acks="all",
        retries=5,
        linger_ms=200,
    )
    try:
        for row in rows:
            producer.send(
                TOPIC,
                key=f"{row['station_uuid']}:{row['timeseries']}".encode(),
                value=kafka_record(row, ingested_at),
            )
        producer.flush(timeout=60)
    finally:
        producer.close(timeout=10)
    return len(rows)


@dag(
    dag_id="pegelonline_hourly",
    schedule="7 * * * *",  # seven minutes past every hour
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_tasks=1,
    default_args={"retries": 3, "retry_delay": 60},
    tags=["stardelt", "pegelonline", "streaming"],
)
def pegelonline_hourly():
    @task
    def ingest() -> dict:
        import httpx

        r = httpx.get(STATIONS_URL, timeout=120)
        r.raise_for_status()
        rows = rows_from(r.json())
        if not rows:
            print("no measurements in the payload")
            return {"rows": 0, "published": 0}
        ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
        # Publish before the append: a Kafka failure then fails the run with nothing written, so
        # the retry cannot duplicate bronze rows. A duplicated message is the documented
        # at-least-once posture for an append-only sink (design/streaming.md §4.5); a duplicated
        # row in bronze is not. The same `ingested_at` goes into both paths, so the dashboard can
        # compare them.
        published = publish_rows(rows, ingested_at)
        table = _ensure_table()
        table.append(arrow_table(rows, ingested_at))
        print(f"appended {len(rows)} rows, newest measurement {max(row['measured_at'] for row in rows)}")
        return {"rows": len(rows), "published": published}

    ingest()


pegelonline_hourly()
