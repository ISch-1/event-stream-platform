/**
 * Sink bounded context.
 *
 * <p>Consumes Avro events from {@code raw-events}, buffers them into batches,
 * writes them to ClickHouse's {@code events.raw_events} table with
 * dedup-on-merge semantics, and routes structural failures to the DLQ topic.
 */
@org.springframework.modulith.ApplicationModule(displayName = "Sink")
package com.esp.sink;
