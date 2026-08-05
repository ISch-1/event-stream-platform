# 0002 — ClickHouse as the OLAP sink

**Status:** Accepted
**Date:** 2026-07-13

## Context

We need a store that can absorb a high-rate write stream (events) and answer analytical queries like "events per type per minute for the last 24 hours" in tens of milliseconds. Rows are appended, never updated in place. Queries scan large time ranges and aggregate.

## Decision

**ClickHouse** with two tables:

1. `events.raw_events` — the sink table, `ReplacingMergeTree(event_time)` engine, partitioned by day, sorted by `(event_type, event_id)`. `ReplacingMergeTree` dedupes on the sort key at merge time, so the same event replayed twice does not survive compaction.
2. `events.events_per_minute` — an incremental **materialized view** on top of `raw_events`, `SummingMergeTree` engine, partitioned by day, sorted by `(event_type, bucket)`. Rows are aggregated automatically as raw events land — the dashboard reads pre-computed rollups.

Retention: `ALTER TABLE ... DROP PARTITION` per day, ~O(ms) per partition.

## Consequences

**Positive**
- Columnar layout + compression puts a full day of events in a few hundred megabytes.
- Aggregations run against `events_per_minute` and answer in milliseconds regardless of raw-events cardinality.
- Native protocol port (9000) gives Grafana's ClickHouse datasource fast, typed access.

**Negative**
- Deduplication is "eventually merged". A query before merge may see the duplicate. Mitigated by `FINAL` or `argMax` when strict correctness is required; the ingest path prevents most duplicates upstream anyway (idempotent producer, dedup at consumer).
- No `UPDATE`/`DELETE` in the OLTP sense. Not an issue for an append-only pipeline.
- Consuming ClickHouse operationally is different from Postgres — separate skill.

## Alternatives considered

1. **Postgres with `INSERT ... ON CONFLICT`.** Fine at 1k rows/s, wall-hits at 10k. Rejected because the point is a high-rate ingest platform.
2. **Elasticsearch.** Full-text friendly, terrible at aggregations over many time buckets, expensive JVM heap footprint.
3. **BigQuery / Redshift / Snowflake.** Cloud-only, defeats the "runs locally on my laptop" property that keeps the demo honest.
4. **Kafka + `KTable` view + interactive query.** Interesting but leaves the data locked inside Kafka. External BI tools would need extra glue.

## References

- [ClickHouse docs — ReplacingMergeTree](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replacingmergetree)
- [Materialized views in ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/create/view#materialized-view)
