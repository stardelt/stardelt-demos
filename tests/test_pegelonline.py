from datetime import datetime

from stardelt_pipelines.pegelonline import COLUMNS, arrow_schema, arrow_table, kafka_record, rows_from

FIXTURE = [
    {
        "uuid": "b475386c-30cc-453a-b3b7-1d17ace13595",
        "number": "48300105",
        "shortname": "CELLE",
        "km": 1.74,
        "agency": "VERDEN",
        "longitude": 10.062164,
        "latitude": 52.622706,
        "water": {"shortname": "ALLER", "longname": "ALLER"},
        "timeseries": [
            {
                "shortname": "W",
                "unit": "cm",
                "currentMeasurement": {
                    "timestamp": "2026-11-06T22:45:00+01:00",
                    "value": 122.0,
                    "stateMnwMhw": "normal",
                    "stateNswHsw": "normal",
                },
            },
            {
                "shortname": "WT",
                "unit": "°C",
                "currentMeasurement": {"timestamp": "2026-11-06T22:30:00+01:00", "value": 9.4},
            },
        ],
    },
    {
        "uuid": "0f7g2b1a-1111-2222-3333-444455556666",
        "number": "50000105",
        "shortname": "DRESDEN",
        "km": 0.0,
        "agency": "DRESDEN",
        "water": {"shortname": "ELBE", "longname": "ELBE"},
        "timeseries": [
            {
                "shortname": "W",
                "unit": "cm",
                "currentMeasurement": {
                    "timestamp": "2026-11-06T22:45:00+01:00",
                    "value": 210.0,
                    "stateMnwMhw": "high",
                },
            },
            {"shortname": "WT", "unit": "°C"},
        ],
    },
    {
        "uuid": "99999999-aaaa-bbbb-cccc-dddddddddddd",
        "number": "60000105",
        "shortname": "OHNE-WERT",
        "agency": "X",
        "water": {"shortname": "RHEIN", "longname": "RHEIN"},
        "timeseries": [],
    },
]


def test_rows_from_returns_three_rows_in_payload_order():
    rows = rows_from(FIXTURE)
    assert len(rows) == 3
    assert rows[0]["station_name"] == "CELLE"
    assert rows[1]["station_name"] == "CELLE"
    assert rows[2]["station_name"] == "DRESDEN"


def test_celle_w_measured_at_is_utc_naive():
    rows = rows_from(FIXTURE)
    celle_w = [r for r in rows if r["station_name"] == "CELLE" and r["timeseries"] == "W"][0]
    assert celle_w["measured_at"] == datetime(2026, 11, 6, 21, 45)


def test_wt_row_values():
    rows = rows_from(FIXTURE)
    wt = [r for r in rows if r["timeseries"] == "WT"][0]
    assert wt["measurement_value"] == 9.4
    assert isinstance(wt["measurement_value"], float)
    assert wt["state_nsw_hsw"] is None


def test_no_timeseries_or_missing_measurement_contribute_nothing():
    rows = rows_from(FIXTURE)
    assert len(rows) == 3
    # assert exact 3 rows as per requirement
    assert [r["station_name"] + ":" + r["timeseries"] for r in rows] == [
        "CELLE:W",
        "CELLE:WT",
        "DRESDEN:W",
    ]


def test_arrow_columns_match_the_table_schema_order_and_nullability():
    """The ten required columns are the ones the Iceberg schema marks `required=True`.

    pyiceberg 0.12 refuses an append whose arrow field is nullable where the table field is
    required, and that refusal is the whole PEGELONLINE failure of stardelt-demos#48.
    """
    rows = rows_from(FIXTURE)
    assert len(rows) == 3
    assert [name for name, _spec, _nullable in COLUMNS] == [
        "station_uuid",
        "station_number",
        "station_name",
        "water",
        "agency",
        "km",
        "latitude",
        "longitude",
        "timeseries",
        "unit",
        "measured_at",
        "measurement_value",
        "state_mnw_mhw",
        "state_nsw_hsw",
        "ingested_at",
    ]
    required = [name for name, _spec, nullable in COLUMNS if not nullable]
    assert required == [
        "station_uuid",
        "station_number",
        "station_name",
        "water",
        "agency",
        "timeseries",
        "unit",
        "measured_at",
        "measurement_value",
        "ingested_at",
    ]


def test_arrow_table_carries_the_declared_nullability():
    pytest = __import__("pytest")
    pa = pytest.importorskip("pyarrow")
    table = arrow_table(rows_from(FIXTURE), datetime(2026, 11, 7, 1, 0))
    assert table.schema == arrow_schema()
    assert [f.name for f in table.schema] == [name for name, _s, _n in COLUMNS]
    for field in table.schema:
        expected = {name: nullable for name, _spec, nullable in COLUMNS}[field.name]
        assert field.nullable is expected, field.name
    assert isinstance(table, pa.Table)


def test_kafka_record_is_one_json_message_with_utc_timestamps():
    import json

    rows = rows_from(FIXTURE)
    message = json.loads(kafka_record(rows[0], datetime(2026, 11, 6, 22, 0)))
    assert message["station_uuid"] == rows[0]["station_uuid"]
    assert message["timeseries"] == rows[0]["timeseries"]
    assert message["measured_at"] == "2026-11-06T21:45:00Z"
    assert message["ingested_at"] == "2026-11-06T22:00:00Z"
    assert isinstance(message["measurement_value"], float)
