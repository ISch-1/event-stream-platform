-- ClickHouse schema.
--
-- ReplacingMergeTree deduplicates on the ORDER BY key when parts merge — we
-- key by event_id so replaying the same message a second time (at-least-once
-- consumer semantics) doesn't inflate the row count. `event_time` is the
-- version column: on collision the row with the higher event_time wins.
--
-- Partitioning by day gives cheap deletes for retention (`ALTER TABLE ... DROP
-- PARTITION`) and prunes queries with a time filter.

CREATE DATABASE IF NOT EXISTS events;

CREATE TABLE IF NOT EXISTS events.raw_events (
    event_id     UUID,
    event_type   LowCardinality(String),
    user_id      String,
    event_time   DateTime64(3) DEFAULT now64(3),
    ingested_at  DateTime64(3) DEFAULT now64(3),
    payload_json String CODEC(ZSTD(1)),
    source_topic LowCardinality(String),
    kafka_partition Int32,
    kafka_offset Int64
) ENGINE = ReplacingMergeTree(event_time)
  PARTITION BY toYYYYMMDD(event_time)
  ORDER BY (event_type, event_id)
  SETTINGS index_granularity = 8192;

-- Aggregate view materialized incrementally as raw_events land.
-- Rebuilds from `POPULATE` — restart the app is safe.
CREATE MATERIALIZED VIEW IF NOT EXISTS events.events_per_minute
ENGINE = SummingMergeTree
PARTITION BY toYYYYMMDD(bucket)
ORDER BY (event_type, bucket)
AS
SELECT
    event_type,
    toStartOfMinute(event_time) AS bucket,
    count() AS event_count
FROM events.raw_events
GROUP BY event_type, bucket;

-- Dead-letter table — populated by the DLQ consumer.
CREATE TABLE IF NOT EXISTS events.dead_letter (
    received_at   DateTime64(3) DEFAULT now64(3),
    source_topic  LowCardinality(String),
    kafka_partition Int32,
    kafka_offset  Int64,
    error_class   LowCardinality(String),
    error_message String,
    raw_bytes_b64 String CODEC(ZSTD(1))
) ENGINE = MergeTree
  PARTITION BY toYYYYMMDD(received_at)
  ORDER BY received_at
  TTL toDateTime(received_at) + INTERVAL 30 DAY;
