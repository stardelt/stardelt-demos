-- The StreamApp does not create its sink table. Run this once as the catalog owner
-- (`mobility-analytics`) before applying the StreamApp. Both timestamp columns are
-- `timestamp with time zone` so the ISO-8601 `Z` form the producer writes parses
-- directly (operator#319 also handles the `timestamp` form; this keeps the demo
-- independent of that build).
CREATE TABLE IF NOT EXISTS mobility.bronze.pegelonline_continuous (
    station_uuid varchar NOT NULL,
    station_number varchar NOT NULL,
    station_name varchar NOT NULL,
    water varchar NOT NULL,
    agency varchar NOT NULL,
    km double,
    latitude double,
    longitude double,
    timeseries varchar NOT NULL,
    unit varchar NOT NULL,
    measured_at timestamp(6) with time zone NOT NULL,
    measurement_value double NOT NULL,
    state_mnw_mhw varchar,
    state_nsw_hsw varchar,
    ingested_at timestamp(6) with time zone NOT NULL
)
WITH (format = 'PARQUET');
