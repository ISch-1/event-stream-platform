# 0003 — Idempotent producer, dedup at the sink

**Status:** Accepted
**Date:** 2026-07-13

## Context

There are two independent failure modes that both cause duplicate rows in the sink:
1. The producer thinks a send failed, retries, and Kafka commits both copies.
2. The consumer processes a batch, writes to ClickHouse, then crashes before committing offsets. On restart it re-processes the batch.

Naïve consumers with `enable.auto.commit=false` and manual offset commit after the write still cannot avoid (2), because the ClickHouse insert and the Kafka offset commit are two separate systems.

## Decision

Defend at both layers:

1. **Producer**: `enable.idempotence=true`, `acks=all`, `max.in.flight.requests.per.connection=5`. Kafka assigns a producer id + sequence, drops retries that carry a sequence it has already seen. That kills failure (1) at source.

2. **Sink**: `ReplacingMergeTree(event_time)` on `events.raw_events`, sorted by `(event_type, event_id)`. ClickHouse eventually merges duplicates when parts merge. Two writes of the same `event_id` compact into one row. That masks failure (2) without adding a distributed transaction.

3. **Consumer**: offsets committed **after** the ClickHouse batch insert returns success. Uses Spring Kafka's `AckMode.MANUAL_IMMEDIATE`. On crash, the batch replays; the sink dedupes.

For queries that need immediate exact-once correctness (before merge), use `FINAL` or `argMax(..., event_time)`.

## Consequences

**Positive**
- End-to-end delivery is at-least-once at the wire, exactly-once at the observed data.
- No distributed transactions, no XA, no Kafka transactions across ClickHouse — none of which exist in a useful way anyway.

**Negative**
- Between an unlucky retry and the next merge, `SELECT COUNT(*)` overestimates. Callers that need exact counts must use `FINAL` (slower) or query `events_per_minute` (materialized view — always aggregated).
- `event_id` is now load-bearing. A producer that reuses ids across genuinely-distinct events corrupts data. The Avro schema documents this.

## Alternatives considered

1. **Kafka transactions + read-committed consumer + transactional ClickHouse insert.** ClickHouse's native protocol has no transactions over an ingestion session in the way Kafka expects; the semantics don't line up.
2. **`INSERT` with `ON CONFLICT` in Postgres.** Trivial, but see ADR 0002 — Postgres is the wrong store for the rate.
3. **A dedup table (id → seen-at) in the consumer.** Adds another moving part; `ReplacingMergeTree` gives us the same thing for free.

## References

- [Confluent — Idempotent Kafka producer](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html#producer-configs-enable-idempotence)
- [Spring Kafka — AckMode](https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/message-listener-container.html#committing-offsets)
