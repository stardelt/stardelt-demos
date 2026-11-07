"""PEGELONLINE (German federal waterway gauges) helpers — pure, stdlib only at import time."""
from __future__ import annotations

import json
from datetime import datetime, timezone

STATIONS_URL = (
    "https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations.json"
    "?includeTimeseries=true&includeCurrentMeasurement=true"
)


def rows_from(payload: list[dict]) -> list[dict]:
    rows: list[dict] = []
    for station in payload:
        station_uuid = station.get("uuid")
        station_number = station.get("number")
        station_name = station.get("shortname")
        water = (station.get("water") or {}).get("shortname") or ""
        agency = station.get("agency") or ""
        km = station.get("km")
        latitude = station.get("latitude")
        longitude = station.get("longitude")
        for ts in station.get("timeseries") or []:
            cm = ts.get("currentMeasurement")
            if not cm:
                continue
            value = cm.get("value")
            if value is None:
                continue
            measured_at_raw = cm.get("timestamp")
            measured_at = None
            if measured_at_raw is not None:
                dt = datetime.fromisoformat(measured_at_raw)
                measured_at = dt.astimezone(timezone.utc).replace(tzinfo=None)
            row = {
                "station_uuid": station_uuid,
                "station_number": station_number,
                "station_name": station_name,
                "water": water,
                "agency": agency,
                "km": km,
                "latitude": latitude,
                "longitude": longitude,
                "timeseries": ts.get("shortname"),
                "unit": ts.get("unit"),
                "measured_at": measured_at,
                "measurement_value": value,
                "state_mnw_mhw": cm.get("stateMnwMhw"),
                "state_nsw_hsw": cm.get("stateNswHsw"),
            }
            rows.append(row)
    return rows


# The arrow columns of `arrow_table`, in the table's order: (name, arrow type, nullable).
# `nullable` mirrors the Iceberg schema that `pegelonline_hourly._ensure_table` declares —
# pyiceberg refuses the whole append when an arrow field is nullable where the table field is
# required ("Mismatch in fields", stardelt-demos#48). Keep the two lists in step.
COLUMNS: tuple[tuple[str, str, bool], ...] = (
    ("station_uuid", "string", False),
    ("station_number", "string", False),
    ("station_name", "string", False),
    ("water", "string", False),
    ("agency", "string", False),
    ("km", "float64", True),
    ("latitude", "float64", True),
    ("longitude", "float64", True),
    ("timeseries", "string", False),
    ("unit", "string", False),
    ("measured_at", "timestamp", False),
    ("measurement_value", "float64", False),
    ("state_mnw_mhw", "string", True),
    ("state_nsw_hsw", "string", True),
    ("ingested_at", "timestamp", False),
)


def arrow_schema():
    """The pyarrow schema for the table: required columns are `nullable=False`.

    `pa.table({...})` infers every field as nullable, which pyiceberg 0.12 rejects against the
    table's required fields; the append needs this schema explicitly.
    """
    import pyarrow as pa

    def field_type(spec: str):
        return pa.timestamp("us") if spec == "timestamp" else getattr(pa, spec)()

    return pa.schema(
        [pa.field(name, field_type(spec), nullable=nullable) for name, spec, nullable in COLUMNS]
    )


def arrow_table(rows: list[dict], ingested_at: datetime):
    import pyarrow as pa

    station_uuid = []
    station_number = []
    station_name = []
    water = []
    agency = []
    km = []
    latitude = []
    longitude = []
    timeseries = []
    unit = []
    measured_at = []
    measurement_value = []
    state_mnw_mhw = []
    state_nsw_hsw = []
    ingested_at_col = []

    for r in rows:
        station_uuid.append(r["station_uuid"])
        station_number.append(r["station_number"])
        station_name.append(r["station_name"])
        water.append(r["water"])
        agency.append(r["agency"])
        km.append(r["km"])
        latitude.append(r["latitude"])
        longitude.append(r["longitude"])
        timeseries.append(r["timeseries"])
        unit.append(r["unit"])
        measured_at.append(r["measured_at"])
        measurement_value.append(r["measurement_value"])
        state_mnw_mhw.append(r.get("state_mnw_mhw"))
        state_nsw_hsw.append(r.get("state_nsw_hsw"))
        ingested_at_col.append(ingested_at)

    return pa.table(
        {
            "station_uuid": pa.array(station_uuid, type=pa.string()),
            "station_number": pa.array(station_number, type=pa.string()),
            "station_name": pa.array(station_name, type=pa.string()),
            "water": pa.array(water, type=pa.string()),
            "agency": pa.array(agency, type=pa.string()),
            "km": pa.array(km, type=pa.float64()),
            "latitude": pa.array(latitude, type=pa.float64()),
            "longitude": pa.array(longitude, type=pa.float64()),
            "timeseries": pa.array(timeseries, type=pa.string()),
            "unit": pa.array(unit, type=pa.string()),
            "measured_at": pa.array(measured_at, type=pa.timestamp("us")),
            "measurement_value": pa.array(measurement_value, type=pa.float64()),
            "state_mnw_mhw": pa.array(state_mnw_mhw, type=pa.string()),
            "state_nsw_hsw": pa.array(state_nsw_hsw, type=pa.string()),
            "ingested_at": pa.array(ingested_at_col, type=pa.timestamp("us")),
        },
        schema=arrow_schema(),
    )


def kafka_record(row: dict, ingested_at: datetime) -> bytes:
    """One measurement as the JSON a `StreamApp` reads.

    The table's fields, with both timestamps as ISO-8601 UTC strings (`…Z`). The key a producer
    derives from it is `station_uuid:timeseries`, which is stable across runs — the sink merges
    on it (D42 §4.5).
    """
    message = dict(row)
    message["measured_at"] = row["measured_at"].isoformat() + "Z"
    message["ingested_at"] = ingested_at.isoformat() + "Z"
    return json.dumps(message, separators=(",", ":")).encode()
