package com.esp.sink.writer;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.esp.avro.Event;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Batches events into a single JDBC {@code INSERT ... VALUES(?,?,?,...)}
 * with a batch call to ClickHouse. Cheaper by orders of magnitude than
 * per-row inserts and lines up with ClickHouse's part-based storage.
 *
 * <p>Duplicates on {@code (event_type, event_id)} are collapsed by ClickHouse
 * at merge time — see ADR 0003.
 */
@Component
@Slf4j
public class ClickHouseWriter {

    private static final String INSERT_SQL = """
            INSERT INTO events.raw_events
                (event_id, event_type, user_id, event_time, ingested_at,
                 payload_json, source_topic, kafka_partition, kafka_offset)
            VALUES (?, ?, ?, fromUnixTimestamp64Milli(?), now64(3), ?, ?, ?, ?)
            """;

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final Timer insertTimer;
    private final io.micrometer.core.instrument.Counter batchCounter;
    private final io.micrometer.core.instrument.Counter rowCounter;

    public ClickHouseWriter(JdbcTemplate jdbc, ObjectMapper mapper, MeterRegistry meters) {
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.insertTimer = Timer.builder("esp.sink.insert")
                .description("Time to insert a batch into ClickHouse")
                .publishPercentileHistogram()
                .register(meters);
        this.batchCounter = meters.counter("esp.sink.batches.written");
        this.rowCounter   = meters.counter("esp.sink.rows.written");
    }

    /** Inserts a whole batch in one JDBC round-trip. Returns rows written. */
    public int write(List<ConsumerRecord<String, Event>> batch) {
        if (batch.isEmpty()) return 0;
        return insertTimer.record(() -> {
            int written = jdbc.execute(INSERT_SQL, (PreparedStatement ps) -> {
                for (ConsumerRecord<String, Event> rec : batch) {
                    Event e = rec.value();
                    bind(ps, rec, e);
                    ps.addBatch();
                }
                int[] r = ps.executeBatch();
                return r.length;
            });
            batchCounter.increment();
            rowCounter.increment(written);
            log.debug("ClickHouse batch: {} rows", written);
            return written;
        });
    }

    private void bind(PreparedStatement ps, ConsumerRecord<String, Event> rec, Event e) throws SQLException {
        int i = 1;
        ps.setString(i++, e.getEventId().toString());
        ps.setString(i++, e.getEventType());
        ps.setString(i++, e.getUserId());
        ps.setLong  (i++, e.getEventTime().toEpochMilli());   // epoch millis, converted in SQL
        ps.setString(i++, payloadJson(e));
        ps.setString(i++, rec.topic());
        ps.setInt   (i++, rec.partition());
        ps.setLong  (i,   rec.offset());
    }

    private String payloadJson(Event e) {
        // avro-maven-plugin uses stringType=String, so keys and values are plain Strings.
        Map<String, String> raw = e.getPayload();
        if (raw == null || raw.isEmpty()) return "{}";
        try {
            return mapper.writeValueAsString(raw);
        } catch (JsonProcessingException ex) {
            log.warn("Payload for event {} could not be serialised to JSON: {}", e.getEventId(), ex.getMessage());
            return "{}";
        }
    }
}
