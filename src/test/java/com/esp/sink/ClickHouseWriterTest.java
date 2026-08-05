package com.esp.sink;

import java.util.List;
import java.util.Map;

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
        long now = System.currentTimeMillis();
        var records = List.of(
                record(Event.newBuilder()
                        .setEventId(java.util.UUID.randomUUID().toString())
                        .setEventType("t")
                        .setUserId("u")
                        .setEventTime(now)
                        .setPayload(Map.of("k", "v"))
                        .setSource("test")
                        .build(), 0L),
                record(Event.newBuilder()
                        .setEventId(java.util.UUID.randomUUID().toString())
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
    }

    private ConsumerRecord<String, Event> record(Event e, long offset) {
        return new ConsumerRecord<>("raw-events", 0, offset, e.getUserId().toString(), e);
    }
}
