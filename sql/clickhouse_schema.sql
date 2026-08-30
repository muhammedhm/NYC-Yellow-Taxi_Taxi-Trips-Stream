-- =====================================================================
-- ClickHouse schema for the real-time taxi stream (Task 3)
--
-- Two tables, two different engines on purpose:
--   raw_trip_events   MergeTree     -- pure append, one row per event ever
--                                       consumed, kept for replay/audit
--   trip_window_agg   ReplacingMergeTree -- "upserted" sliding-window
--                                       snapshots; same (window_end,
--                                       pu_location_id) key gets replaced
--                                       by the newest write during merges
--
-- Apply with:
--   docker exec -i taxi_clickhouse clickhouse-client --user taxi --password taxi \
--       --multiquery < sql/clickhouse_schema.sql
-- =====================================================================

CREATE DATABASE IF NOT EXISTS taxi_stream;

-- ---------------------------------------------------------------------
-- Raw events, append-only, one row per trip consumed from Kafka.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS taxi_stream.raw_trip_events
(
    trip_seq            UInt64,
    vendor_id            Int32,
    pickup_datetime       DateTime,
    dropoff_datetime      DateTime,
    passenger_count       Float64,
    trip_distance         Float64,
    pu_location_id        Int32,
    do_location_id        Int32,
    rate_code_id          Int32,
    payment_type          Int32,
    fare_amount           Float64,
    tip_amount            Float64,
    total_amount          Float64,
    producer_sent_at      DateTime64(3),
    consumed_at           DateTime64(3) DEFAULT now64(3),
    is_late_arrival       UInt8 DEFAULT 0   -- flagged by the consumer's watermark check
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(pickup_datetime)
ORDER BY (pickup_datetime, trip_seq);

-- ---------------------------------------------------------------------
-- Sliding-window aggregation snapshots. The consumer recomputes and
-- upserts one row per (pu_location_id, window_end) every 60 seconds,
-- covering the trailing 5-minute window ending at that timestamp.
-- ReplacingMergeTree(updated_at) means if the same window somehow gets
-- recomputed and re-inserted (e.g. consumer restart), the row with the
-- latest `updated_at` wins once ClickHouse merges parts.
--
-- IMPORTANT: ClickHouse only de-duplicates during background merges, so
-- a query run right after inserts may still see duplicate rows for the
-- same key. Use `FINAL` (or `OPTIMIZE TABLE ... FINAL`) when reading if
-- you need guaranteed-deduplicated results immediately, e.g.:
--   SELECT * FROM taxi_stream.trip_window_agg FINAL ORDER BY window_end;
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS taxi_stream.trip_window_agg
(
    window_start         DateTime,
    window_end           DateTime,
    pu_location_id        Int32,
    trip_count            UInt64,
    total_revenue          Float64,
    avg_fare               Float64,
    late_events_in_window  UInt64,
    updated_at             DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (pu_location_id, window_end);
