package com.esp.ingest.application;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.esp.avro.Event;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

/**
 * Converts a validated {@link com.esp.ingest.api.IngestRequest} to an Avro
 * {@link Event} and produces it. Send is synchronous with a bounded await so
 * a failing broker surfaces as a 5xx to the client rather than piling up
 * in-flight requests.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class IngestService {

    private static final long SEND_TIMEOUT_SECONDS = 2;

    private final KafkaTemplate<String, Event> producer;
    private final MeterRegistry meters;

    @Value("${esp.topics.raw}")
    private String topic;

    private Counter accepted(String type) {
        return Counter.builder("esp.ingest.accepted")
                .description("Events successfully produced to Kafka")
                .tag("event_type", type)
                .register(meters);
    }

    private Counter rejected(String reason) {
        return Counter.builder("esp.ingest.rejected")
                .description("Events the ingest layer refused to publish")
                .tag("reason", reason)
                .register(meters);
    }

    public IngestOutcome accept(String type,
                                String userId,
                                UUID eventId,
                                Long eventTimeEpochMs,
                                Map<String, String> payload,
                                String source) {
        UUID id = eventId != null ? eventId : UUID.randomUUID();
        long timestamp = eventTimeEpochMs != null ? eventTimeEpochMs : Instant.now().toEpochMilli();

        Event avro = Event.newBuilder()
                .setEventId(id)
                .setEventType(type)
                .setUserId(userId)
                .setEventTime(Instant.ofEpochMilli(timestamp))
                .setPayload(payload == null ? Map.of() : Map.copyOf(payload))
                .setSource(source)
                .build();

        try {
            SendResult<String, Event> result = producer
                    .send(topic, userId, avro)                      // partition by userId — same user's events stay ordered
                    .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            RecordMetadata md = result.getRecordMetadata();

            accepted(type).increment();
            return new IngestOutcome(id, topic, md.partition(), md.offset());
        } catch (java.util.concurrent.TimeoutException e) {
            rejected("timeout").increment();
            throw new IllegalStateException("Kafka producer timed out — broker unhealthy?", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            rejected("interrupted").increment();
            throw new IllegalStateException("Interrupted while producing event " + id, e);
        } catch (java.util.concurrent.ExecutionException e) {
            rejected("broker").increment();
            log.error("Kafka rejected event {}: {}", id, e.getMessage());
            throw new IllegalStateException("Kafka rejected the event", e);
        }
    }

    public record IngestOutcome(UUID eventId, String topic, int partition, long offset) {}
}
