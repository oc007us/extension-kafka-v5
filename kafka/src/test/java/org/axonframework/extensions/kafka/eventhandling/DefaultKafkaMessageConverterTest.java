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
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.conversion.Converter;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DefaultKafkaMessageConverterTest {

    private Converter converter;
    private DefaultKafkaMessageConverter messageConverter;

    @BeforeEach
    void setUp() {
        converter = mock(Converter.class);
        when(converter.convert(any(), eq(byte[].class)))
                .thenAnswer(inv -> inv.getArgument(0).toString().getBytes());
        when(converter.convert(any(byte[].class), eq(Object.class)))
                .thenAnswer(inv -> new String((byte[]) inv.getArgument(0)));

        messageConverter = DefaultKafkaMessageConverter.builder()
                                                       .converter(converter)
                                                       .build();
    }

    @Test
    void createKafkaMessageProducesValidProducerRecord() {
        EventMessage event = new GenericEventMessage(
                "msg-123",
                new MessageType("com.example.MyEvent", "1"),
                "test-payload",
                Metadata.with("aggregateIdentifier", "agg-1"),
                Instant.ofEpochMilli(1700000000000L)
        );

        ProducerRecord<String, byte[]> record = messageConverter.createKafkaMessage(event, "test-topic");

        assertThat(record.topic()).isEqualTo("test-topic");
        assertThat(record.key()).isEqualTo("agg-1");
        assertThat(new String(record.value())).isEqualTo("test-payload");

        Headers headers = record.headers();
        assertThat(HeaderUtils.valueAsString(headers, KafkaHeaders.MESSAGE_ID)).isEqualTo("msg-123");
        assertThat(HeaderUtils.valueAsString(headers, KafkaHeaders.MESSAGE_TYPE)).isEqualTo("com.example.MyEvent");
        assertThat(HeaderUtils.valueAsString(headers, KafkaHeaders.MESSAGE_REVISION)).isEqualTo("1");
        assertThat(HeaderUtils.valueAsLong(headers, KafkaHeaders.MESSAGE_TIMESTAMP)).isEqualTo(1700000000000L);
    }

    @Test
    void createKafkaMessageOmitsDefaultRevision() {
        EventMessage event = new GenericEventMessage(
                "msg-456",
                new MessageType("com.example.Event"),
                "payload",
                Metadata.emptyInstance(),
                Instant.now()
        );

        ProducerRecord<String, byte[]> record = messageConverter.createKafkaMessage(event, "topic");

        assertThat(HeaderUtils.valueAsString(headers(record), KafkaHeaders.MESSAGE_REVISION)).isNull();
    }

    @Test
    void createKafkaMessageUsesMessageIdAsKeyWhenNoAggregateId() {
        EventMessage event = new GenericEventMessage(
                "msg-789",
                new MessageType("com.example.Event"),
                "payload",
                Metadata.emptyInstance(),
                Instant.now()
        );

        ProducerRecord<String, byte[]> record = messageConverter.createKafkaMessage(event, "topic");
        assertThat(record.key()).isEqualTo("msg-789");
    }

    @Test
    void roundTripConversion() {
        EventMessage original = new GenericEventMessage(
                "msg-roundtrip",
                new MessageType("com.example.RoundTrip", "2"),
                "hello-world",
                Metadata.with("customKey", "customVal")
                        .and("aggregateIdentifier", "agg-42"),
                Instant.ofEpochMilli(1700000000000L)
        );

        ProducerRecord<String, byte[]> kafkaMessage = messageConverter.createKafkaMessage(original, "roundtrip-topic");

        ConsumerRecord<String, byte[]> consumerRecord = new ConsumerRecord<>(
                "roundtrip-topic", 0, 0L,
                kafkaMessage.key(), kafkaMessage.value()
        );
        kafkaMessage.headers().forEach(h -> consumerRecord.headers().add(h));

        Optional<EventMessage> result = messageConverter.readKafkaMessage(consumerRecord);

        assertThat(result).isPresent();
        EventMessage restored = result.get();
        assertThat(restored.identifier()).isEqualTo("msg-roundtrip");
        assertThat(restored.type().qualifiedName().toString()).isEqualTo("com.example.RoundTrip");
        assertThat(restored.type().version()).isEqualTo("2");
        assertThat(restored.timestamp()).isEqualTo(Instant.ofEpochMilli(1700000000000L));
        assertThat(restored.metadata().get("customKey")).isEqualTo("customVal");
    }

    @Test
    void readKafkaMessageReturnsEmptyForNonAxonRecord() {
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>("topic", 0, 0L, "key", "body".getBytes());

        assertThat(messageConverter.readKafkaMessage(record)).isEmpty();
    }

    @Test
    void readKafkaMessageReturnsEmptyWhenMissingRequiredHeaders() {
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>("topic", 0, 0L, "key", "body".getBytes());
        record.headers().add(KafkaHeaders.MESSAGE_ID, "id".getBytes());
        record.headers().add(KafkaHeaders.MESSAGE_TYPE, "type".getBytes());
        // Missing MESSAGE_TIMESTAMP

        assertThat(messageConverter.readKafkaMessage(record)).isEmpty();
    }

    @Test
    void builderRequiresConverter() {
        assertThatThrownBy(() -> DefaultKafkaMessageConverter.builder().build())
                .isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void builderAcceptsCustomKeyFunction() {
        DefaultKafkaMessageConverter custom = DefaultKafkaMessageConverter.builder()
                .converter(converter)
                .keyFunction(e -> "fixed-key")
                .build();

        EventMessage event = new GenericEventMessage(
                "id", new MessageType("Type"), "payload", Metadata.emptyInstance(), Instant.now()
        );

        assertThat(custom.createKafkaMessage(event, "topic").key()).isEqualTo("fixed-key");
    }

    @Test
    void createKafkaMessageIncludesAggregateHeaders() {
        Metadata metadata = Metadata.with("aggregateType", "OrderAggregate")
                                    .and("aggregateIdentifier", "order-1")
                                    .and("aggregateSequenceNumber", "5");
        EventMessage event = new GenericEventMessage(
                "msg-agg", new MessageType("com.example.OrderPlaced"), "payload", metadata, Instant.now()
        );

        ProducerRecord<String, byte[]> record = messageConverter.createKafkaMessage(event, "topic");

        assertThat(HeaderUtils.valueAsString(headers(record), KafkaHeaders.AGGREGATE_TYPE))
                .isEqualTo("OrderAggregate");
        assertThat(HeaderUtils.valueAsString(headers(record), KafkaHeaders.AGGREGATE_ID))
                .isEqualTo("order-1");
        assertThat(HeaderUtils.valueAsLong(headers(record), KafkaHeaders.AGGREGATE_SEQ))
                .isEqualTo(5L);
    }

    private static Headers headers(ProducerRecord<?, ?> record) {
        return record.headers();
    }
}
