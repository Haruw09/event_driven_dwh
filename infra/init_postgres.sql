-- RAW events table
CREATE TABLE IF NOT EXISTS raw_events (
  event_id         uuid PRIMARY KEY,
  event_time       timestamptz NOT NULL,
  ingestion_time   timestamptz NOT NULL,
  event_name       text        NOT NULL,
  user_id          bigint      NOT NULL,
  session_id       uuid        NOT NULL,
  product_id       bigint,
  price            numeric(12,2),
  device           text,
  payload          jsonb       NOT NULL
);

-- helpful indexes for queries / incremental loads
CREATE INDEX IF NOT EXISTS idx_raw_events_ingestion_time ON raw_events (ingestion_time);
CREATE INDEX IF NOT EXISTS idx_raw_events_event_time ON raw_events (event_time);
CREATE INDEX IF NOT EXISTS idx_raw_events_event_name ON raw_events (event_name);

-- Ingestion audit table
CREATE TABLE IF NOT EXISTS ingestion_runs (
  run_id           uuid PRIMARY KEY,
  file_name        text        NOT NULL,
  started_at       timestamptz NOT NULL,
  finished_at      timestamptz,
  rows_in_file     integer     NOT NULL,
  rows_loaded      integer     NOT NULL,
  rows_deduped     integer     NOT NULL,
  status           text        NOT NULL,
  error_message    text
);

CREATE INDEX IF NOT EXISTS idx_ingestion_runs_started_at ON ingestion_runs (started_at);
