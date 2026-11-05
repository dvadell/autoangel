CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS samples (
  series_id uuid NOT NULL,
  ts timestamptz NOT NULL,
  value double precision NOT NULL
);

SELECT create_hypertable('samples', 'ts', if_not_exists => TRUE);
