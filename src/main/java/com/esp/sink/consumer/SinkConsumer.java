package com.esp.sink.consumer;

import java.util.List;

import com.esp.avro.Event;
import com.esp.sink.dlq.DeadLetterPublisher;
import com.esp.sink.writer.ClickHouseWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Batched consumer for the raw-events topic.
 *
 * <p>Contract with Kafka:
 * <ul>
 *   <li>Manual ack ({@code AckMode.MANUAL_IMMEDIATE}) — commit offsets only
 *       after ClickHouse acknowledges the batch.</li>
 *   <li>{@code max.poll.records=500} — bounded batch size.</li>
 * </ul>
 *
 * <p>Failure modes:
 * <ul>
 *   <li>Transient DB error → do <b>not</b> ack, throw. Spring Kafka's error
 *       handler retries the batch with backoff.</li>
 *   <li>Individual record shaped wrong (e.g. missing required field) → route
 *       that single record to the DLQ, keep going with the rest of the batch.</li>
 * </ul>
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class SinkConsumer {

    private final ClickHouseWriter writer;
    private final DeadLetterPublisher dlq;

    @KafkaListener(
            topics = "${esp.topics.raw}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory")
    public void onBatch(List<ConsumerRecord<String, Event>> batch, Acknowledgment ack) {
        if (batch.isEmpty()) {
            ack.acknowledge();
            return;
        }

        List<ConsumerRecord<String, Event>> good = new java.util.ArrayList<>(batch.size());
        for (ConsumerRecord<String, Event> rec : batch) {
            if (rec.value() == null) {
                // Tombstones or deserialisation-null (ErrorHandlingDeserializer would have handled a real error).
                dlq.send(rec, new IllegalStateException("null value — treated as poison"));
                continue;
            }
            if (rec.value().getEventId() == null || rec.value().getEventType() == null) {
                dlq.send(rec, new IllegalArgumentException("missing required field"));
                continue;
            }
            good.add(rec);
        }

        if (good.isEmpty()) {
            ack.acknowledge();
            return;
        }

        try {
            writer.write(good);
        } catch (DataAccessException e) {
            log.error("ClickHouse batch insert failed — will retry (batch size={})", good.size(), e);
            throw e;    // Spring Kafka's DefaultErrorHandler will back off and re-deliver.
        }

        ack.acknowledge();
    }
}
