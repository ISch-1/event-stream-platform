package com.esp.sink.consumer;

import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Container factory dedicated to the sink consumer:
 * <ul>
 *   <li>Batch listener — receives {@code List<ConsumerRecord>} in one call.</li>
 *   <li>Manual ack — offsets committed by the listener after ClickHouse.</li>
 *   <li>Backoff on retryable failures (1s × 5); after that the batch is
 *       recovered by the error handler which routes to the DLQ per record.</li>
 * </ul>
 *
 * <p>The byte-oriented producer bean lives here too — used only by the DLQ
 * publisher, which needs to forward the raw bytes of poison messages.
 */
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    private static final String SCHEMA_REGISTRY_URL_KEY = "schema.registry.url";

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, com.esp.avro.Event>
    kafkaListenerContainerFactory(KafkaProperties props) {
        Map<String, Object> consumerConfig = new HashMap<>(props.buildConsumerProperties());
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        // Pull the schema-registry URL from spring.kafka.properties (the dotted
        // key can't be read via @Value directly).
        Object srUrl = props.getProperties().getOrDefault(SCHEMA_REGISTRY_URL_KEY, "http://localhost:8081");
        consumerConfig.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, srUrl);
        consumerConfig.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        var factory = new ConcurrentKafkaListenerContainerFactory<String, com.esp.avro.Event>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(consumerConfig));
        factory.setBatchListener(true);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(1_000L, 5L)));
        factory.setConcurrency(3);
        return factory;
    }

    /** Byte-oriented producer for the DLQ (must forward raw payloads). */
    @Bean
    public KafkaTemplate<byte[], byte[]> byteTemplate(KafkaProperties props) {
        Map<String, Object> cfg = new HashMap<>(props.buildProducerProperties());
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(cfg));
    }
}
