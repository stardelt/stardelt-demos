from datetime import datetime

from stardelt_pipelines.pegelonline import rows_from

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
