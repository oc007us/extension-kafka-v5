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

package org.axonframework.extensions.kafka.eventhandling.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
class KafkaPublisherTest {

    private ProducerFactory<String, byte[]> producerFactory;
    private Producer<String, byte[]> producer;
    private KafkaMessageConverter<String, byte[]> messageConverter;

    @BeforeEach
    void setUp() {
        producerFactory = mock(ProducerFactory.class);
        producer = mock(Producer.class);
        messageConverter = mock(KafkaMessageConverter.class);

        when(producerFactory.createProducer()).thenReturn(producer);
        when(producerFactory.confirmationMode()).thenReturn(ConfirmationMode.NONE);
    }

    @Test
    void sendPublishesEventToKafka() {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>("topic", "key", "value".getBytes());
        when(messageConverter.createKafkaMessage(any(), any())).thenReturn(record);
        CompletableFuture<RecordMetadata> future = CompletableFuture.completedFuture(
                new RecordMetadata(new TopicPartition("topic", 0), 0, 0, 0, 0, 0)
        );
        when(producer.send(any())).thenReturn(future);

        KafkaPublisher<String, byte[]> publisher = KafkaPublisher.<String, byte[]>builder()
                .producerFactory(producerFactory)
                .messageConverter(messageConverter)
                .build();

        publisher.send(testEvent(), null);

        verify(producer).send(record);
        verify(producer).close();
    }

    @Test
    void sendSkipsWhenTopicResolverReturnsEmpty() {
        KafkaPublisher<String, byte[]> publisher = KafkaPublisher.<String, byte[]>builder()
                .producerFactory(producerFactory)
                .messageConverter(messageConverter)
                .topicResolver(e -> Optional.empty())
                .build();

        publisher.send(testEvent(), null);

        verify(producerFactory, never()).createProducer();
    }

    @Test
    void sendUsesCustomTopic() {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>("custom-topic", "key", "value".getBytes());
        when(messageConverter.createKafkaMessage(any(), eq("custom-topic"))).thenReturn(record);
        CompletableFuture<RecordMetadata> future = CompletableFuture.completedFuture(
                new RecordMetadata(new TopicPartition("custom-topic", 0), 0, 0, 0, 0, 0)
        );
        when(producer.send(any())).thenReturn(future);

        KafkaPublisher<String, byte[]> publisher = KafkaPublisher.<String, byte[]>builder()
                .producerFactory(producerFactory)
                .messageConverter(messageConverter)
                .topic("custom-topic")
                .build();

        publisher.send(testEvent(), null);

        ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(captor.capture());
        assertThat(captor.getValue().topic()).isEqualTo("custom-topic");
    }

    @Test
    void sendWithTransactionalMode() {
        when(producerFactory.confirmationMode()).thenReturn(ConfirmationMode.TRANSACTIONAL);
        ProducerRecord<String, byte[]> record = new ProducerRecord<>("topic", "key", "value".getBytes());
        when(messageConverter.createKafkaMessage(any(), any())).thenReturn(record);
        CompletableFuture<RecordMetadata> future = CompletableFuture.completedFuture(
                new RecordMetadata(new TopicPartition("topic", 0), 0, 0, 0, 0, 0)
        );
        when(producer.send(any())).thenReturn(future);

        KafkaPublisher<String, byte[]> publisher = KafkaPublisher.<String, byte[]>builder()
                .producerFactory(producerFactory)
                .messageConverter(messageConverter)
                .build();

        publisher.send(testEvent(), null);

        verify(producer).beginTransaction();
        verify(producer).commitTransaction();
        verify(producer).close();
    }

    @Test
    void shutDownDelegatesToProducerFactory() {
        KafkaPublisher<String, byte[]> publisher = KafkaPublisher.<String, byte[]>builder()
                .producerFactory(producerFactory)
                .messageConverter(messageConverter)
                .build();

        publisher.shutDown();

        verify(producerFactory).shutDown();
    }

    @Test
    void builderRequiresProducerFactory() {
        assertThatThrownBy(() -> KafkaPublisher.<String, byte[]>builder()
                .messageConverter(messageConverter)
                .build())
                .isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void builderRequiresMessageConverter() {
        assertThatThrownBy(() -> KafkaPublisher.<String, byte[]>builder()
                .producerFactory(producerFactory)
                .build())
                .isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void builderRejectsNegativeAckTimeout() {
        assertThatThrownBy(() -> KafkaPublisher.<String, byte[]>builder()
                .producerFactory(producerFactory)
                .messageConverter(messageConverter)
                .publisherAckTimeout(-1)
                .build())
                .isInstanceOf(AxonConfigurationException.class);
    }

    private EventMessage testEvent() {
        return new GenericEventMessage(
                "msg-id", new MessageType("TestEvent"), "payload", Metadata.emptyInstance(), Instant.now()
        );
    }
}
