-- Mirrors infra/clickhouse/init.sql for use as a Testcontainers init script.
-- Keep the two files in sync. This copy exists because Testcontainers loads
-- init scripts from the classpath, not the compose bind mount.

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
  ORDER BY (event_type, event_id);

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
  ORDER BY received_at;
