CREATE SCHEMA IF NOT EXISTS mobility.bronze;
CREATE TABLE IF NOT EXISTS mobility.bronze.pegelonline_stream (
  station_uuid varchar NOT NULL, station_number varchar NOT NULL, station_name varchar NOT NULL,
  water varchar NOT NULL, agency varchar NOT NULL, km double, latitude double, longitude double,
  timeseries varchar NOT NULL, unit varchar NOT NULL,
  measured_at timestamp(6) NOT NULL, measurement_value double NOT NULL,
  state_mnw_mhw varchar, state_nsw_hsw varchar, ingested_at timestamp(6) NOT NULL
);
