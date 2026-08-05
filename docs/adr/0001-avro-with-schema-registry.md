# 0001 — Avro + Schema Registry over JSON

**Status:** Accepted
**Date:** 2026-07-13

## Context

Events flow between many producers and consumers written over years. Schemas change. The question is: how do we survive that?

## Decision

Wire format is **Avro binary**, resolved through **Confluent Schema Registry** with **BACKWARD** compatibility.

- Producers write with the latest schema, consumers read with the schema they were compiled against; Schema Registry supplies the intermediate for on-the-fly resolution.
- New fields must be added with a default value. Fields can be removed only if they have a default. Rename ⇒ new field with default + old field deprecated over one release.
- Schema Registry rejects incompatible producers at publish time. A misbehaving producer fails fast in staging, never contaminates the topic.

## Consequences

**Positive**
- Payloads are smaller than JSON (no field names on the wire).
- Consumers cannot silently break on renamed fields. The compatibility rule is enforced by Schema Registry, not by convention.
- Downstream tools that speak Avro (Debezium, Kafka Connect, Flink) work with no extra glue.

**Negative**
- One more moving part in local dev (`schema-registry`).
- Language support outside the JVM ecosystem is uneven — a JS producer needs `avsc` or similar, and the ergonomics are worse than JSON.

## Alternatives considered

1. **JSON + JSON Schema.** Human-readable but larger, and schema evolution rules are per-consumer discipline rather than enforced at the broker boundary. Rejected because "discipline" is not a defence against a rushed change.
2. **Protobuf + Schema Registry.** Also good; would swap easily. Chose Avro because the ClickHouse and Kafka Connect ecosystem still leans slightly more Avro-first.
3. **JSON without a registry.** The default for demos. Rejected — the schema is *in the code*, so a schema change requires a coordinated release across every consumer. Doesn't scale past two teams.

## References

- Kleppmann, *Designing Data-Intensive Applications*, ch. 4
- [Confluent — Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
