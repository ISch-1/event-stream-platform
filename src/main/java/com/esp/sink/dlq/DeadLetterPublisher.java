package com.esp.sink.dlq;

import java.util.Base64;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Routes poison messages to two places:
 *
 * <ol>
 *   <li>The DLT Kafka topic, with error metadata attached as headers so
 *       operators can inspect the payload in the Kafka UI without touching
 *       the pipeline code.</li>
 *   <li>A ClickHouse row in {@code events.dead_letter}, base64-encoded, so
 *       operators can query for failures by class and time window.</li>
 * </ol>
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class DeadLetterPublisher {

    private static final String INSERT_DLQ_SQL = """
            INSERT INTO events.dead_letter
                (source_topic, kafka_partition, kafka_offset,
                 error_class, error_message, raw_bytes_b64)
            VALUES (?, ?, ?, ?, ?, ?)
            """;

    /** Byte-oriented template so we can DLT malformed messages that don't deserialise. */
    private final KafkaTemplate<byte[], byte[]> byteTemplate;

    private final JdbcTemplate clickhouseJdbc;
    private final MeterRegistry meters;

    @Value("${esp.topics.dlq}")
    private String dlqTopic;

    public void send(ConsumerRecord<?, ?> origin, Throwable cause) {
        String errClass = cause.getClass().getName();
        String errMsg = cause.getMessage() != null ? cause.getMessage() : "";
        byte[] rawKey   = origin.key()   instanceof byte[] k ? k   : (origin.key()   == null ? null : origin.key().toString().getBytes());
        byte[] rawValue = origin.value() instanceof byte[] v ? v   : (origin.value() == null ? null : origin.value().toString().getBytes());

        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(dlqTopic, rawKey, rawValue);
        record.headers().add(new RecordHeader("x-error-class", errClass.getBytes()));
        record.headers().add(new RecordHeader("x-error-msg", errMsg.getBytes()));
        record.headers().add(new RecordHeader("x-origin-topic", origin.topic().getBytes()));
        record.headers().add(new RecordHeader("x-origin-partition", String.valueOf(origin.partition()).getBytes()));
        record.headers().add(new RecordHeader("x-origin-offset", String.valueOf(origin.offset()).getBytes()));

        try {
            byteTemplate.send(record).get();
        } catch (Exception e) {
            log.error("Failed to publish to DLQ topic — dropping context row too", e);
        }

        try {
            String b64 = rawValue == null ? "" : Base64.getEncoder().encodeToString(rawValue);
            clickhouseJdbc.update(INSERT_DLQ_SQL,
                    origin.topic(), origin.partition(), origin.offset(),
                    errClass, truncate(errMsg, 4000), b64);
        } catch (Exception e) {
            log.warn("Failed to record DLQ entry in ClickHouse", e);
        }

        meters.counter("esp.dlq.written", "error_class", errClass).increment();
        log.warn("Poison message routed to DLQ — topic={} partition={} offset={} class={}",
                origin.topic(), origin.partition(), origin.offset(), errClass);
    }

    private static String truncate(String s, int max) {
        if (s == null) return null;
        return s.length() <= max ? s : s.substring(0, max);
    }
}
