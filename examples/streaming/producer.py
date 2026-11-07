"""A continuous PEGELONLINE-shaped producer for the streaming demo.

Unlike the hourly `pegelonline_hourly` DAG (a poller for a source that only changes
hourly), this pod writes one measurement per station every few seconds, so the
StreamApp has a genuinely continuous input. It is a demo workload, not part of the
platform.
"""
from __future__ import annotations

import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP", "kafka-kafka-bootstrap.stardelt.svc.cluster.local:9092"
)
TOPIC = os.environ.get("TOPIC", "pegelonline.continuous")
INTERVAL_SECONDS = float(os.environ.get("INTERVAL_SECONDS", "5"))

# A few real PEGELONLINE station shapes; the values are synthetic.
STATIONS = [
    {
        "station_uuid": "b475386c-30cc-453a-b3b7-1d17ace13595",
        "station_number": "48300105",
        "station_name": "CELLE",
        "water": "ALLER",
        "agency": "VERDEN",
        "km": 1.74,
        "latitude": 52.622706,
        "longitude": 10.062164,
    },
    {
        "station_uuid": "4e6f7a8b-91c2-4d3e-8f45-6a7b8c9d0e1f",
        "station_number": "48900100",
        "station_name": "DRESDEN",
        "water": "ELBE",
        "agency": "DRESDEN",
        "km": 0.0,
        "latitude": 51.0504,
        "longitude": 13.7383,
    },
    {
        "station_uuid": "9a8b7c6d-5e4f-4a3b-2c1d-0e9f8a7b6c5d",
        "station_number": "57000100",
        "station_name": "KOBLENZ",
        "water": "RHEIN",
        "agency": "KOBLENZ",
        "km": 0.0,
        "latitude": 50.3569,
        "longitude": 7.5889,
    },
]


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        acks="all",
        retries=5,
        linger_ms=50,
    )
    print(
        f"producing {len(STATIONS)} rows every {INTERVAL_SECONDS:g}s "
        f"to {TOPIC} at {BOOTSTRAP}",
        flush=True,
    )
    sent = 0
    try:
        while True:
            now = datetime.now(timezone.utc)
            measured_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            ingested_at = now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            for station in STATIONS:
                row = {
                    **station,
                    "timeseries": "W",
                    "unit": "cm",
                    "measured_at": measured_at,
                    "measurement_value": round(random.uniform(45.0, 320.0), 1),
                    "state_mnw_mhw": "normal",
                    "state_nsw_hsw": "normal",
                    "ingested_at": ingested_at,
                }
                producer.send(
                    TOPIC,
                    key=f"{station['station_uuid']}:W".encode(),
                    value=json.dumps(row, separators=(",", ":")).encode(),
                )
            producer.flush(timeout=10)
            sent += len(STATIONS)
            print(f"{ingested_at} sent {len(STATIONS)} rows (total {sent})", flush=True)
            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        pass
    finally:
        producer.close(timeout=10)


if __name__ == "__main__":
    main()
