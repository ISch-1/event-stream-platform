package com.esp.sink;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.esp.avro.Event;
import com.esp.sink.writer.ClickHouseWriter;
import com.esp.support.IntegrationTestBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Direct unit(-ish) test of the ClickHouse writer, bypassing Kafka. Bound to
 * {@code *Test.java} so it runs in Surefire (unit), not Failsafe (IT) — but
 * still uses the shared Testcontainers ClickHouse. Cheap once containers are up.
 */
@DisplayName("ClickHouseWriter")
class ClickHouseWriterTest extends IntegrationTestBase {

    @Autowired ClickHouseWriter writer;

    @Autowired
    @Qualifier("clickhouseJdbc")
    JdbcTemplate clickhouse;

    @Test
    @DisplayName("writes a batch and returns row count")
    void writesBatch() {
        // Millisecond precision: that is what Avro's timestamp-millis and DateTime64(3) keep.
        Instant now = Instant.ofEpochMilli(System.currentTimeMillis());
        UUID firstId = UUID.randomUUID();
        var records = List.of(
                record(Event.newBuilder()
                        .setEventId(firstId)
                        .setEventType("t")
                        .setUserId("u")
                        .setEventTime(now)
                        .setPayload(Map.of("k", "v"))
                        .setSource("test")
                        .build(), 0L),
                record(Event.newBuilder()
                        .setEventId(UUID.randomUUID())
                        .setEventType("t")
                        .setUserId("u")
                        .setEventTime(now)
                        .setPayload(Map.of())
                        .setSource("test")
                        .build(), 1L)
        );

        int written = writer.write(records);
        assertThat(written).isGreaterThanOrEqualTo(2);

        Integer count = clickhouse.queryForObject(
                "SELECT count() FROM events.raw_events WHERE event_type = 't' AND user_id = 'u'",
                Integer.class);
        assertThat(count).isGreaterThanOrEqualTo(2);

        // event_time must round-trip exactly, not be read as seconds or shifted by a time zone.
        Long storedMillis = clickhouse.queryForObject(
                "SELECT toUnixTimestamp64Milli(event_time) FROM events.raw_events FINAL WHERE event_id = ?",
                Long.class, firstId.toString());
        assertThat(storedMillis).isEqualTo(now.toEpochMilli());
    }

    private ConsumerRecord<String, Event> record(Event e, long offset) {
        return new ConsumerRecord<>("raw-events", 0, offset, e.getUserId().toString(), e);
    }
}
