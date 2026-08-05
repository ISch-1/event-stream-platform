package com.esp.ingest;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import com.esp.support.IntegrationTestBase;
import com.fasterxml.jackson.databind.JsonNode;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end: HTTP POST → Kafka → ClickHouse.
 *
 * <p>Boots the whole Spring context against real Kafka and ClickHouse. Posts
 * a request, waits until the row lands in ClickHouse, then asserts on the
 * persisted fields.
 */
@DisplayName("Ingest → Kafka → ClickHouse")
class IngestPipelineIT extends IntegrationTestBase {

    @Autowired TestRestTemplate rest;

    @Autowired
    @org.springframework.beans.factory.annotation.Qualifier("clickhouseJdbc")
    JdbcTemplate clickhouse;

    @Test
    @DisplayName("single event lands in raw_events with the right shape")
    void singleEventFlow() {
        UUID id = UUID.randomUUID();
        Map<String, Object> body = Map.of(
                "eventId", id.toString(),
                "type", "page_view",
                "userId", "user-42",
                "payload", Map.of("path", "/home", "referrer", "google")
        );

        ResponseEntity<JsonNode> res = rest.postForEntity("/api/v1/events", body, JsonNode.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(res.getBody().get("eventId").asText()).isEqualTo(id.toString());
        assertThat(res.getBody().get("topic").asText()).isEqualTo("raw-events");
        assertThat(res.getBody().get("offset").asLong()).isGreaterThanOrEqualTo(0);

        // Sink consumer should have persisted the row within a few seconds.
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            Integer count = clickhouse.queryForObject(
                    "SELECT count() FROM events.raw_events FINAL WHERE event_id = ?",
                    Integer.class, id.toString());
            assertThat(count).isEqualTo(1);
        });

        Map<String, Object> row = clickhouse.queryForMap(
                "SELECT event_type, user_id, payload_json, source_topic " +
                "FROM events.raw_events FINAL WHERE event_id = ?", id.toString());
        assertThat(row.get("event_type")).isEqualTo("page_view");
        assertThat(row.get("user_id")).isEqualTo("user-42");
        assertThat(row.get("source_topic")).isEqualTo("raw-events");
        assertThat(row.get("payload_json").toString()).contains("\"path\":\"/home\"");
    }

    @Test
    @DisplayName("replaying the same event_id doesn't double-count (ReplacingMergeTree)")
    void dedupOnReplay() {
        UUID id = UUID.randomUUID();
        Map<String, Object> body = Map.of(
                "eventId", id.toString(),
                "type", "checkout_completed",
                "userId", "user-99",
                "payload", Map.of("total", "42.00")
        );

        // Send the same event three times.
        for (int i = 0; i < 3; i++) {
            var res = rest.postForEntity("/api/v1/events", body, JsonNode.class);
            assertThat(res.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        }

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            // FINAL forces on-the-fly merge — a normal SELECT might still see 3 before compaction.
            Integer countDedup = clickhouse.queryForObject(
                    "SELECT count() FROM events.raw_events FINAL WHERE event_id = ?",
                    Integer.class, id.toString());
            assertThat(countDedup).as("FINAL query sees at most one row per event_id").isEqualTo(1);
        });
    }
}
