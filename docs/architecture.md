# Architecture

## C4 — Level 1: System Context

```mermaid
graph LR
    clients[External producers<br/>Web · mobile · services]
    ops[Ops & analysts<br/>Grafana]
    esp[event-stream-platform<br/>Java 21 · Spring Boot 3]
    monitoring[(Prometheus + Jaeger)]

    clients -->|POST /api/v1/events| esp
    esp -->|OTLP| monitoring
    ops -->|SQL over HTTP| esp
```

## C4 — Level 2: Containers

```mermaid
graph TB
    api[Ingest module]
    sink[Sink module]
    dlq[DLQ writer]

    kafka[(Kafka<br/>raw-events)]
    dltop[(Kafka<br/>raw-events.DLT)]
    sr[(Schema Registry<br/>BACKWARD compat)]
    ch[(ClickHouse<br/>raw_events + MV)]

    clients[HTTP clients] --> api
    api -->|Avro-serialized<br/>idempotent producer| kafka
    api -->|register/lookup| sr
    sink -->|consume| kafka
    sink -->|batched INSERT| ch
    sink -->|structural failures| dltop
    dlq -->|consume| dltop
    dlq -->|write context row| ch
```

## Ingest flow

```mermaid
sequenceDiagram
    autonumber
    participant client as HTTP client
    participant api as IngestController
    participant svc as IngestService
    participant sr as Schema Registry
    participant kafka as Kafka (raw-events)

    client->>api: POST /api/v1/events
    api->>svc: validate + convert to Avro
    svc->>sr: fetch/register Event.avsc
    sr-->>svc: schema id
    svc->>kafka: produce (idempotent, acks=all)
    kafka-->>svc: ack
    svc-->>api: eventId
    api-->>client: 202 Accepted
```

## Sink flow

```mermaid
sequenceDiagram
    autonumber
    participant kafka as Kafka
    participant sink as SinkConsumer
    participant ch as ClickHouse

    loop poll batch
        kafka->>sink: N Avro records
        sink->>sink: deserialize · validate · buffer
        sink->>ch: INSERT ... FORMAT JSONEachRow (batch)
        ch-->>sink: 200
        sink->>kafka: commit offsets (MANUAL_IMMEDIATE)
    end

    Note over sink: Structural failure? → publish to raw-events.DLT and skip
    Note over ch: Same event_id twice? → ReplacingMergeTree collapses on merge
```

## Consistency model

- **At the wire** — at-least-once. Producer retries under idempotence + consumer replays after crash.
- **At ClickHouse** — effectively exactly-once by construction (dedup on `event_id` via `ReplacingMergeTree`). Queries against the materialized view are always aggregated and never observe duplicates.
- **DLQ** — the offset advances on structural failure, so a poison message cannot stall the pipeline.

## Backpressure

- The Kafka producer runs off the request thread, but the `send().get(2s)` await gives us bounded latency and forces a 5xx if the broker is unhealthy — the client stops flooding.
- The sink consumer uses `max.poll.records=500` and manual commit. It cannot outpace ClickHouse's insert rate — the next poll blocks until the previous batch is fully committed.
