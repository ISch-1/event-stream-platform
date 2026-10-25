package com.esp.ingest.infrastructure;

import com.esp.avro.Event;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * The Avro producer the ingest API publishes with.
 *
 * <p>Spring Boot only auto-configures a {@code KafkaTemplate} when none exists,
 * and the sink module declares a {@code byte[]} template for its DLQ, so this
 * one has to be declared explicitly. Its settings still come from
 * {@code spring.kafka.producer} (Avro serializer, idempotence, acks=all).
 */
@Configuration
public class KafkaProducerConfig {

    @Bean
    public KafkaTemplate<String, Event> eventTemplate(KafkaProperties props) {
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props.buildProducerProperties()));
    }
}
