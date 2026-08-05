/**
 * Ingest bounded context.
 *
 * <p>Accepts events over HTTP, validates them, converts to Avro, and produces
 * to the {@code raw-events} Kafka topic with an idempotent producer.
 *
 * <p>External API surface: {@link com.esp.ingest.api}. Application and
 * infrastructure are internal.
 */
@org.springframework.modulith.ApplicationModule(displayName = "Ingest")
package com.esp.ingest;
