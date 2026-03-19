/*
 * Copyright (c) 2010-2026. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.extensions.kafka.eventhandling;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.extensions.kafka.eventhandling.producer.DefaultProducerFactory;
import org.axonframework.extensions.kafka.eventhandling.producer.KafkaPublisher;
import org.axonframework.extensions.kafka.eventhandling.producer.ProducerFactory;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test verifying that {@link KafkaPublisher} can publish events to a real Kafka broker and that those
 * events can be consumed by a raw {@link KafkaConsumer}.
 *
 * @author Axon Framework Contributors
 * @since 5.0
 */
class ProducerIntegrationTest extends KafkaContainerIntegrationTest {

    private static final String TEST_TOPIC = "producer-integration-test";

    private ProducerFactory<String, byte[]> producerFactory;
    private KafkaPublisher<String, byte[]> publisher;
    private DefaultKafkaMessageConverter messageConverter;

    @BeforeEach
    void setUp() {
        Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        producerFactory = DefaultProducerFactory.<String, byte[]>builder()
                                                .configuration(producerConfig)
                                                .build();

        messageConverter = DefaultKafkaMessageConverter.builder()
                                                       .converter(new JacksonConverter())
                                                       .build();

        publisher = KafkaPublisher.<String, byte[]>builder()
                                  .producerFactory(producerFactory)
                                  .messageConverter(messageConverter)
                                  .topic(TEST_TOPIC)
                                  .build();
    }

    @AfterEach
    void tearDown() {
        publisher.shutDown();
    }

    @Test
    void publishedEventCanBeConsumedFromKafka() {
        // given
        String payload = "ProducerTestPayload";
        MessageType messageType = new MessageType(String.class);
        EventMessage event = new GenericEventMessage(messageType, payload);

        // when
        publisher.send(event, null);

        // then
        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "producer-test-group");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerConfig)) {
            consumer.subscribe(Collections.singletonList(TEST_TOPIC));

            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(10));

            assertThat(records.count()).isGreaterThanOrEqualTo(1);

            ConsumerRecord<String, byte[]> record = records.iterator().next();
            Optional<EventMessage> reconstituted = messageConverter.readKafkaMessage(record);
            assertThat(reconstituted).isPresent();
            assertThat(reconstituted.get().payload()).isEqualTo(payload);
        }
    }

    @Test
    void publishedEventContainsCorrectHeaders() {
        // given
        String payload = "HeaderTestPayload";
        MessageType messageType = new MessageType(String.class);
        Map<String, String> metadata = Map.of("myKey", "myValue");
        EventMessage event = new GenericEventMessage(messageType, payload, metadata);

        // when
        publisher.send(event, null);

        // then
        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "header-test-group");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerConfig)) {
            consumer.subscribe(Collections.singletonList(TEST_TOPIC));

            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records.count()).isGreaterThanOrEqualTo(1);

            ConsumerRecord<String, byte[]> record = records.iterator().next();
            assertThat(record.headers().lastHeader(KafkaHeaders.MESSAGE_ID)).isNotNull();
            assertThat(record.headers().lastHeader(KafkaHeaders.MESSAGE_TYPE)).isNotNull();
            assertThat(record.headers().lastHeader(KafkaHeaders.MESSAGE_TIMESTAMP)).isNotNull();
        }
    }
}
