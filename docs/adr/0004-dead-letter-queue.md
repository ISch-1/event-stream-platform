# 0004 — Dead-letter topic for poison messages

**Status:** Accepted
**Date:** 2026-07-13

## Context

A single unparseable message — bad Avro payload, incompatible schema, a bug in the consumer — can stall the pipeline indefinitely. The consumer retries forever, offsets never advance, downstream systems starve.

## Decision

Any message the consumer cannot process **for a structural reason** — deserialization failure, `NullPointerException` on a required field, ClickHouse rejection with a permanent error — is routed to a **dead-letter topic** `raw-events.DLT`. Offset for the origin partition advances immediately.

Retryable failures (network blip, transient ClickHouse timeout) stay on the main topic — the consumer relies on Spring Kafka's `DefaultErrorHandler` with an exponential backoff (`FixedBackOff(1s, 3)` in dev; `ExponentialBackOff` for prod).

Poison messages that reach the DLT also get a row in `events.dead_letter` in ClickHouse with the raw bytes (base64), the error class, and the source offset. This gives operators enough context to inspect, patch, and replay by hand.

## Consequences

**Positive**
- The consumer group cannot deadlock on a single bad message.
- Operators have a queryable, dated view of what went wrong.
- The origin topic stays clean — no "retry hell" tombstones.

**Negative**
- A misclassified failure (retryable dumped to DLT) risks silent data loss. Mitigated by strict rules: only known-permanent exception types skip retry. Everything else retries.
- DLQ replays are manual. Automating that is a v2 story.

## Alternatives considered

1. **Retry forever with a big log line.** Familiar, catastrophic. Rejected — it was the default in one of my previous internships and I saw it kill Sunday afternoons.
2. **Poison-pill sidecar consumer that just logs and drops.** Loses the payload. Rejected — you can never replay it.
3. **A "quarantine" table but the same topic.** Doubles the failure-mode surface, still requires operator intervention. The DLT is more standard and inspectable via the Kafka UI.

## References

- [Spring Kafka — DeadLetterPublishingRecoverer](https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html)
- [Uber — Reliable Reprocessing at Uber](https://www.uber.com/blog/reliable-reprocessing/)
