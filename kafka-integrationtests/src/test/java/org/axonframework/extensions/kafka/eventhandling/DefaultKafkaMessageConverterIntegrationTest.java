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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests verifying that {@link DefaultKafkaMessageConverter} correctly round-trips an
 * {@link EventMessage} through a {@link ProducerRecord} and back via a {@link ConsumerRecord}.
 *
 * @author Axon Framework Contributors
 * @since 5.0
 */
class DefaultKafkaMessageConverterIntegrationTest {

    private static final String TEST_TOPIC = "test-topic";

    private DefaultKafkaMessageConverter converter;

    @BeforeEach
    void setUp() {
        converter = DefaultKafkaMessageConverter.builder()
                                                .converter(new JacksonConverter())
                                                .build();
    }

    @Test
    void roundTripWithPayloadAndMetadata() {
        // given
        String payload = "Hello Kafka";
        Map<String, String> metadataMap = Map.of("traceId", "abc-123", "customKey", "customValue");
        Metadata metadata = Metadata.from(metadataMap);
        MessageType messageType = new MessageType(String.class);
        EventMessage originalEvent = new GenericEventMessage(messageType, payload, metadata);

        // when
        ProducerRecord<String, byte[]> producerRecord = converter.createKafkaMessage(originalEvent, TEST_TOPIC);

        ConsumerRecord<String, byte[]> consumerRecord = new ConsumerRecord<>(
                producerRecord.topic(),
                0,
                0L,
                producerRecord.key(),
                producerRecord.value()
        );
        // Copy headers from producer record to consumer record
        producerRecord.headers().forEach(header -> consumerRecord.headers().add(header));

        Optional<EventMessage> result = converter.readKafkaMessage(consumerRecord);

        // then
        assertThat(result).isPresent();
        EventMessage reconstituted = result.get();
        assertThat(reconstituted.payload()).isEqualTo(payload);
        assertThat(reconstituted.identifier()).isEqualTo(originalEvent.identifier());
        assertThat(reconstituted.type().qualifiedName()).isEqualTo(messageType.qualifiedName());
        assertThat(reconstituted.metadata().get("traceId")).isEqualTo("abc-123");
        assertThat(reconstituted.metadata().get("customKey")).isEqualTo("customValue");
    }

    @Test
    void roundTripPreservesTimestamp() {
        // given
        String payload = "TimestampTest";
        MessageType messageType = new MessageType(String.class);
        Instant fixedTimestamp = Instant.parse("2026-01-15T10:30:00Z");
        EventMessage originalEvent = new GenericEventMessage(
                "msg-id-123", messageType, payload, Map.of(), fixedTimestamp
        );

        // when
        ProducerRecord<String, byte[]> producerRecord = converter.createKafkaMessage(originalEvent, TEST_TOPIC);

        ConsumerRecord<String, byte[]> consumerRecord = new ConsumerRecord<>(
                producerRecord.topic(), 0, 0L,
                producerRecord.key(), producerRecord.value()
        );
        producerRecord.headers().forEach(header -> consumerRecord.headers().add(header));

        Optional<EventMessage> result = converter.readKafkaMessage(consumerRecord);

        // then
        assertThat(result).isPresent();
        assertThat(result.get().timestamp()).isEqualTo(fixedTimestamp);
    }

    @Test
    void roundTripPreservesAggregateMetadata() {
        // given
        String payload = "AggregateEvent";
        MessageType messageType = new MessageType(String.class);
        Map<String, String> metadataMap = Map.of(
                "aggregateType", "OrderAggregate",
                "aggregateIdentifier", "order-42",
                "aggregateSequenceNumber", "7"
        );
        EventMessage originalEvent = new GenericEventMessage(messageType, payload, metadataMap);

        // when
        ProducerRecord<String, byte[]> producerRecord = converter.createKafkaMessage(originalEvent, TEST_TOPIC);

        ConsumerRecord<String, byte[]> consumerRecord = new ConsumerRecord<>(
                producerRecord.topic(), 0, 0L,
                producerRecord.key(), producerRecord.value()
        );
        producerRecord.headers().forEach(header -> consumerRecord.headers().add(header));

        Optional<EventMessage> result = converter.readKafkaMessage(consumerRecord);

        // then
        assertThat(result).isPresent();
        EventMessage reconstituted = result.get();
        assertThat(reconstituted.metadata().get("aggregateType")).isEqualTo("OrderAggregate");
        assertThat(reconstituted.metadata().get("aggregateIdentifier")).isEqualTo("order-42");
        assertThat(reconstituted.metadata().get("aggregateSequenceNumber")).isEqualTo("7");
    }

    @Test
    void nonAxonRecordReturnsEmpty() {
        // given
        ConsumerRecord<String, byte[]> foreignRecord = new ConsumerRecord<>(
                TEST_TOPIC, 0, 0L, "key", "some-bytes".getBytes()
        );

        // when
        Optional<EventMessage> result = converter.readKafkaMessage(foreignRecord);

        // then
        assertThat(result).isEmpty();
    }
}
