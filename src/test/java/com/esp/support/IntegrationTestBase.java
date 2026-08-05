package com.esp.support;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Singleton Kafka + ClickHouse containers. Schema Registry is mocked in-process
 * via Confluent's {@code mock://} URL scheme — see ADR 0005 and
 * {@code application-test.yml}.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@Testcontainers
public abstract class IntegrationTestBase {

    protected static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.1"))
                    .withReuse(true);

    protected static final ClickHouseContainer CLICKHOUSE =
            new ClickHouseContainer(DockerImageName.parse("clickhouse/clickhouse-server:24.9-alpine"))
                    .withUsername("esp")
                    .withPassword("esp")
                    .withInitScript("clickhouse-init.sql")
                    .withReuse(true);

    static {
        KAFKA.start();
        CLICKHOUSE.start();
    }

    @DynamicPropertySource
    static void wire(DynamicPropertyRegistry reg) {
        reg.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        reg.add("esp.clickhouse.url",             () -> CLICKHOUSE.getJdbcUrl().replace("/default", "/events"));
        reg.add("esp.clickhouse.user",            CLICKHOUSE::getUsername);
        reg.add("esp.clickhouse.password",        CLICKHOUSE::getPassword);
    }
}
