# 0005 — Testcontainers for the streaming test suite

**Status:** Accepted
**Date:** 2026-07-13

## Context

Streaming tests need Kafka, Schema Registry, and ClickHouse. Mocks are worse than useless here — the interesting bugs live in serialization corners, offset semantics, and dedup behaviour. Only the real systems exhibit them.

## Decision

Every `*IT` test uses **Testcontainers** with singleton containers reused across the JVM. Kafka via `org.testcontainers.kafka.KafkaContainer`, Schema Registry (we point tests at a mock in-process implementation from `io.confluent.kafka.schemaregistry.testutil` to avoid a second container), ClickHouse via `org.testcontainers.clickhouse.ClickHouseContainer`.

Ryuk is disabled in CI (containers get reaped when the ephemeral runner terminates); it stays enabled for local dev.

## Consequences

**Positive**
- Tests exercise the same Avro wire format, the same ClickHouse SQL, the same ReplacingMergeTree behaviour that production sees.
- Regression tests for tricky bugs (a `NullPointerException` on a nullable Avro field, a dedup that failed because `event_id` was not in the sort key) become simple JUnit tests.

**Negative**
- ~15–25 seconds of container startup per JVM. Amortised across the suite.
- Requires Docker on developer laptops and on CI runners. Documented in the README.

## Alternatives considered

1. **`EmbeddedKafkaBroker`.** Works, but no Schema Registry integration, and its API diverges from real Kafka in subtle ways (offset commit timing, rebalance events).
2. **A shared dev broker.** Kills parallel test execution and creates cross-engineer flakiness. Rejected in ADR 0005 of the sister project (ecommerce-polyglot) for the same reason.
3. **Full stack in `docker compose` before tests.** Slow; wrong tool for hermetic tests.

## References

- [Testcontainers — Kafka module](https://java.testcontainers.org/modules/kafka/)
- [Confluent — MockSchemaRegistryClient](https://docs.confluent.io/platform/current/schema-registry/develop/serdes-schema-reg.html#mock-schema-registry)
