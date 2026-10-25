# event-stream-platform

[![CI](https://github.com/ISch-1/event-stream-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/ISch-1/event-stream-platform/actions/workflows/ci.yml)
[![Java](https://img.shields.io/badge/Java-21-orange)](https://openjdk.org/projects/jdk/21/)
[![Spring Boot](https://img.shields.io/badge/Spring_Boot-3.3-brightgreen)](https://spring.io/projects/spring-boot)
[![Kafka](https://img.shields.io/badge/Kafka-3.7-black)](https://kafka.apache.org/)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-24.9-yellow)](https://clickhouse.com/)
[![License](https://img.shields.io/badge/License-MIT-blue)](LICENSE)

> A data-intensive event pipeline: HTTP ingest → Avro on Kafka → sink to ClickHouse. Focused on the parts that make streaming hard — schema evolution, idempotent producers, exactly-once-at-the-sink via dedup, dead-letter handling for poison messages, and observability tuned for streaming rather than request/response.

The point isn't to demo Kafka. The point is: **every design decision that shows up in the code is explained in an ADR and validated by an executable integration test.** A recruiter or staff engineer can open one file and understand the whole thing.

---

## What's implemented

| Bounded context | Status | Storage / topic |
|---|---|---|
| **Ingest** | ✅ HTTP + Avro + idempotent producer | Kafka `raw-events` |
| **Sink**   | ✅ Batched consumer + ClickHouse writer + DLQ | ClickHouse `events.raw_events` + `raw-events.DLT` |
| Stream aggregations (Kafka Streams) | 🚧 planned | Kafka `events.per-minute` |
| Threshold alerter | 🚧 planned | Kafka `alerts` |

The `events.events_per_minute` materialized view already runs in ClickHouse — the Kafka Streams aggregator will be an alternative pre-computation path.

---

## Design at a glance

- **Avro + Schema Registry** with BACKWARD compatibility. Producers cannot break consumers with a rename. See [ADR 0001](docs/adr/0001-avro-with-schema-registry.md).
- **ClickHouse as the OLAP sink**, `ReplacingMergeTree` for dedup, incremental materialized view for per-minute rollups. See [ADR 0002](docs/adr/0002-clickhouse-as-olap-sink.md).
- **Idempotent producer** (`enable.idempotence=true`, `acks=all`) plus **dedup at the sink** on `(event_type, event_id)`. End-to-end effectively exactly-once at the observed data, at-least-once at the wire. See [ADR 0003](docs/adr/0003-idempotent-producer-and-dedup-sink.md).
- **Dead-letter topic** for structural failures + a ClickHouse `dead_letter` table for post-mortem queries. Offsets on the origin partition always advance. See [ADR 0004](docs/adr/0004-dead-letter-queue.md).
- **Testcontainers** for real Kafka + ClickHouse in tests; Schema Registry mocked in-process via Confluent's `mock://` URL. See [ADR 0005](docs/adr/0005-testcontainers-for-streaming.md).
- **Observability from day one** — request rate & latency, consumer lag, DLQ rate, ClickHouse insert p95, JVM heap. Grafana dashboard versioned as JSON.

Full sequence diagrams live in [docs/architecture.md](docs/architecture.md).

---

## Quickstart

Prerequisites: JDK 21, Docker (Compose v2), `curl`, `jq`. Optional: `k6`.

The Maven wrapper (`./mvnw`) is committed, so a JDK is all you need to build.

```bash
make up        # Kafka + Schema Registry + ClickHouse + Prometheus + Grafana + Jaeger + Kafka UI
make run       # start the app (in another terminal)

# Send a few events
make seed

# Query ClickHouse
docker exec esp-clickhouse clickhouse-client --user esp --password esp \
  --query "SELECT event_type, count() FROM events.raw_events FINAL GROUP BY event_type ORDER BY count() DESC"
```

Landmarks:

- Ingest API — <http://localhost:8080/api/v1/events>
- Swagger UI — <http://localhost:8080/docs>
- Kafka UI — <http://localhost:8085> (browse topics, messages, schemas)
- Schema Registry — <http://localhost:8081/subjects>
- ClickHouse Playground — <http://localhost:8123/play>
- Grafana — <http://localhost:3000> → dashboard "Pipeline — Overview"
- Prometheus — <http://localhost:9090>
- Jaeger — <http://localhost:16686>

---

## Running the tests

```bash
make test      # unit tests
make it        # integration tests (Testcontainers spins up Kafka + ClickHouse)
```

The critical test is [`IngestPipelineIT`](src/test/java/com/esp/ingest/IngestPipelineIT.java):

1. `POST /api/v1/events` → 202 with `eventId`.
2. Sink consumer reads from Kafka.
3. Row appears in `events.raw_events` within seconds — assertion checks columns.
4. **Replay the same event 3 times → `SELECT count() FROM ... FINAL` returns 1.** The dedup guarantee, verified against a real ClickHouse.

---

## API

**POST `/api/v1/events`** — publish a single event.

```json
{
  "eventId": "3f...uuid...",
  "type":    "page_view",
  "userId":  "user-42",
  "eventTimeEpochMs": 1730000000000,
  "payload": { "path": "/home", "referrer": "google" },
  "source":  "web"
}
```

`eventId`, `eventTimeEpochMs`, and `source` are optional — the server fills them in. Responses:

- **202 Accepted** — `{ "eventId": "...", "topic": "raw-events", "partition": 0, "offset": 12345 }`
- **400** — validation failure, structured `ApiError`
- **500** — broker unhealthy (bounded 2s producer await, surfaces fast)

Same error shape as `ecommerce-polyglot`:

```json
{
  "status": 400,
  "code": "validation.failed",
  "message": "Request body failed validation",
  "details": [{ "field": "type", "message": "must not be blank" }],
  "traceId": "3ad9c3a4...",
  "timestamp": "2026-07-13T12:34:56.789Z"
}
```

---

## Handy queries once events are flowing

```bash
# Volume by type
docker exec esp-clickhouse clickhouse-client --user esp --password esp -q "
SELECT event_type, count() AS n
FROM events.raw_events FINAL
WHERE event_time > now() - INTERVAL 15 MINUTE
GROUP BY event_type ORDER BY n DESC"

# Per-minute aggregation (already materialized)
docker exec esp-clickhouse clickhouse-client --user esp --password esp -q "
SELECT bucket, event_type, sum(event_count) AS n
FROM events.events_per_minute
WHERE bucket > now() - INTERVAL 15 MINUTE
GROUP BY bucket, event_type ORDER BY bucket DESC, n DESC LIMIT 30"

# DLQ inspection
docker exec esp-clickhouse clickhouse-client --user esp --password esp -q "
SELECT received_at, source_topic, kafka_offset, error_class, error_message
FROM events.dead_letter ORDER BY received_at DESC LIMIT 20"
```

Kafka side:

```bash
# List topics (raw-events, raw-events.DLT appear after the app starts)
make topics

# Consume live events
docker exec -it esp-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic raw-events --from-beginning \
  --property print.key=true

# Registered Avro schemas
make schemas
```

---

## Load test

```bash
make smoke        # k6: ramps to 200 VUs, sustained 1 min against POST /api/v1/events
```

Thresholds enforced: `p95 < 150 ms`, error rate `< 1%`. Watch the Grafana "Pipeline — Overview" dashboard in real time while it runs: ingest RPS, consumer lag, ClickHouse batches/s.

---

## Repo layout

```
event-stream-platform/
├── docker-compose.yml           # Kafka, Schema Registry, ClickHouse, observability, Kafka UI
├── Makefile
├── pom.xml
│
├── docs/
│   ├── architecture.md          # C4 + sequence diagrams
│   └── adr/                     # 5 ADRs
│
├── infra/
│   ├── clickhouse/init.sql      # DB, raw_events, materialized view, dead_letter
│   ├── prometheus/prometheus.yml
│   ├── grafana/                 # provisioned datasources (Prom + Jaeger + ClickHouse) + dashboard
│   └── otel/otel-collector-config.yaml
│
├── src/main/
│   ├── avro/Event.avsc          # wire schema — Schema Registry compatibility BACKWARD
│   ├── java/com/esp/
│   │   ├── EventStreamPlatformApplication.java
│   │   ├── ingest/
│   │   │   ├── api/             # HTTP layer
│   │   │   └── application/     # IngestService — idempotent producer
│   │   ├── sink/
│   │   │   ├── consumer/        # Batched Kafka listener + container factory
│   │   │   ├── writer/          # ClickHouse batch writer
│   │   │   └── dlq/             # DeadLetterPublisher (Kafka + ClickHouse row)
│   │   └── shared/              # errors, MDC filter
│   └── resources/
│       ├── application.yml
│       ├── application-dev.yml
│       └── logback-spring.xml
│
├── src/test/
│   ├── java/com/esp/
│   │   ├── ModularityTest.java
│   │   ├── ingest/IngestPipelineIT.java   # HTTP → Kafka → ClickHouse + dedup replay
│   │   ├── sink/ClickHouseWriterTest.java # writer against real ClickHouse
│   │   └── support/IntegrationTestBase.java
│   └── resources/
│       ├── application-test.yml
│       └── clickhouse-init.sql            # test-only DDL (mirrors infra/clickhouse/init.sql)
│
└── load/ingest-smoke.js
```

---

## Roadmap

- [x] Ingest → Avro → Kafka with idempotent producer
- [x] Sink consumer → ClickHouse with dedup on `event_id`
- [x] DLQ (Kafka topic + ClickHouse dead-letter table)
- [x] Observability: Prometheus + Grafana + OTel + Jaeger
- [x] Integration tests with real Kafka + ClickHouse
- [ ] **Kafka Streams aggregator** — per-user event count over 1-minute tumbling windows, sink to `events.per-minute` topic
- [ ] **Threshold alerter** — consume the aggregation topic, emit alerts to `alerts` topic when thresholds exceeded
- [ ] **Schema evolution demo** — commit an Avro schema v2 with a new optional field and test that old consumers keep working
- [ ] **gRPC ingest endpoint** — same producer path, different transport, for high-throughput internal clients

---

## Why this project exists

Alongside [ecommerce-polyglot](https://github.com/ISch-1/ecommerce-polyglot), this repo covers the other half of a senior backend interview: not "how do you model a domain" but "how do you move a lot of data reliably". Two independent but complementary demos of applied distributed systems, both boot-in-one-command, both observable.

## License

MIT — see [LICENSE](LICENSE).
