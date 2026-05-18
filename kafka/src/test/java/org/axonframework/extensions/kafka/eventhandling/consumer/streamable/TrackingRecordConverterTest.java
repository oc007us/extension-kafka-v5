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

package org.axonframework.extensions.kafka.eventhandling.consumer.streamable;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TrackingRecordConverterTest {

    @SuppressWarnings("unchecked")
    @Test
    void convertAdvancesTokenAndReturnsKafkaEventMessages() {
        KafkaMessageConverter<String, byte[]> messageConverter = mock(KafkaMessageConverter.class);
        EventMessage event = new GenericEventMessage(
                "id-1", new MessageType("TestEvent"), "payload", Metadata.emptyInstance(), Instant.now()
        );
        when(messageConverter.readKafkaMessage(any())).thenReturn(Optional.of(event));

        KafkaTrackingToken startToken = KafkaTrackingToken.emptyToken();
        TrackingRecordConverter<String, byte[]> converter = new TrackingRecordConverter<>(messageConverter, startToken);

        TopicPartition tp = new TopicPartition("topic", 0);
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>("topic", 0, 5L, "key", "value".getBytes());
        Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> recordMap = new HashMap<>();
        recordMap.put(tp, List.of(record));
        ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(recordMap);

        List<KafkaEventMessage> result = converter.convert(records);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).offset()).isEqualTo(5L);
        assertThat(result.get(0).partition()).isEqualTo(0);
        assertThat(result.get(0).value()).isSameAs(event);

        KafkaTrackingToken currentToken = converter.currentToken();
        assertThat(currentToken.getPositions()).containsEntry(tp, 5L);
    }

    @SuppressWarnings("unchecked")
    @Test
    void convertSkipsUnconvertibleRecords() {
        KafkaMessageConverter<String, byte[]> messageConverter = mock(KafkaMessageConverter.class);
        when(messageConverter.readKafkaMessage(any())).thenReturn(Optional.empty());

        TrackingRecordConverter<String, byte[]> converter =
                new TrackingRecordConverter<>(messageConverter, KafkaTrackingToken.emptyToken());

        TopicPartition tp = new TopicPartition("topic", 0);
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>("topic", 0, 0L, "key", "value".getBytes());
        Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> recordMap = new HashMap<>();
        recordMap.put(tp, List.of(record));
        ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(recordMap);

        List<KafkaEventMessage> result = converter.convert(records);

        assertThat(result).isEmpty();
        assertThat(converter.currentToken().getPositions()).isEmpty();
    }

    @SuppressWarnings("unchecked")
    @Test
    void convertMultipleRecordsAdvancesTokenIncrementally() {
        KafkaMessageConverter<String, byte[]> messageConverter = mock(KafkaMessageConverter.class);
        EventMessage event = new GenericEventMessage(
                "id", new MessageType("TestEvent"), "payload", Metadata.emptyInstance(), Instant.now()
        );
        when(messageConverter.readKafkaMessage(any())).thenReturn(Optional.of(event));

        TrackingRecordConverter<String, byte[]> converter =
                new TrackingRecordConverter<>(messageConverter, KafkaTrackingToken.emptyToken());

        TopicPartition tp0 = new TopicPartition("topic", 0);
        TopicPartition tp1 = new TopicPartition("topic", 1);
        Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> recordMap = new HashMap<>();
        recordMap.put(tp0, List.of(
                new ConsumerRecord<>("topic", 0, 0L, "k", "v".getBytes()),
                new ConsumerRecord<>("topic", 0, 1L, "k", "v".getBytes())
        ));
        recordMap.put(tp1, List.of(
                new ConsumerRecord<>("topic", 1, 0L, "k", "v".getBytes())
        ));
        ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(recordMap);

        List<KafkaEventMessage> result = converter.convert(records);

        assertThat(result).hasSize(3);
        KafkaTrackingToken currentToken = converter.currentToken();
        assertThat(currentToken.getPositions().get(tp0)).isEqualTo(1L);
        assertThat(currentToken.getPositions().get(tp1)).isEqualTo(0L);
    }
}
