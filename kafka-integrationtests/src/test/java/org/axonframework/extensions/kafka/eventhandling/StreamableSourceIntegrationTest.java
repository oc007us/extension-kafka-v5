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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.extensions.kafka.eventhandling.consumer.AsyncFetcher;
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.DefaultConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.Fetcher;
import org.axonframework.extensions.kafka.eventhandling.consumer.streamable.KafkaEventMessage;
import org.axonframework.extensions.kafka.eventhandling.consumer.streamable.KafkaTrackingToken;
import org.axonframework.extensions.kafka.eventhandling.consumer.streamable.StreamableKafkaEventSource;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end integration test verifying that events published to Kafka can be consumed through the
 * {@link StreamableKafkaEventSource}.
 *
 * @author Axon Framework Contributors
 * @since 5.0
 */
class StreamableSourceIntegrationTest extends KafkaContainerIntegrationTest {

    private static final String TEST_TOPIC = "streamable-source-test";

    private DefaultKafkaMessageConverter messageConverter;
    private StreamableKafkaEventSource<String, byte[]> eventSource;
    private Fetcher<String, byte[], KafkaEventMessage> fetcher;

    @BeforeEach
    void setUp() {
        messageConverter = DefaultKafkaMessageConverter.builder()
                                                       .converter(new JacksonConverter())
                                                       .build();

        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        ConsumerFactory<String, byte[]> consumerFactory = new DefaultConsumerFactory<>(consumerConfig);

        fetcher = AsyncFetcher.<String, byte[], KafkaEventMessage>builder()
                              .pollTimeout(1_000)
                              .build();

        eventSource = StreamableKafkaEventSource.<String, byte[]>builder()
                                                .consumerFactory(consumerFactory)
                                                .fetcher(fetcher)
                                                .messageConverter(messageConverter)
                                                .topics(Collections.singletonList(TEST_TOPIC))
                                                .build();
    }

    @AfterEach
    void tearDown() {
        fetcher.shutdown();
    }

    @Test
    void eventsPublishedToKafkaAppearInStream() {
        // given
        publishTestEvents(3);

        // when
        MessageStream<EventMessage> stream = eventSource.open(
                StreamingCondition.startingFrom(null), null
        );

        // then
        List<EventMessage> received = new ArrayList<>();
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            Optional<MessageStream.Entry<EventMessage>> entry = stream.next();
            entry.ifPresent(e -> received.add(e.message()));
            assertThat(received).hasSizeGreaterThanOrEqualTo(3);
        });

        assertThat(received.get(0).payload()).isEqualTo("event-0");
        assertThat(received.get(1).payload()).isEqualTo("event-1");
        assertThat(received.get(2).payload()).isEqualTo("event-2");

        stream.close();
    }

    @Test
    void streamEntriesContainTrackingTokenInContext() {
        // given
        publishTestEvents(1);

        // when
        MessageStream<EventMessage> stream = eventSource.open(
                StreamingCondition.startingFrom(null), null
        );

        // then
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            Optional<MessageStream.Entry<EventMessage>> entry = stream.next();
            assertThat(entry).isPresent();

            MessageStream.Entry<EventMessage> streamEntry = entry.get();
            Optional<TrackingToken> token = TrackingToken.fromContext(streamEntry);
            assertThat(token).isPresent();
            assertThat(token.get()).isInstanceOf(KafkaTrackingToken.class);

            KafkaTrackingToken kafkaToken = (KafkaTrackingToken) token.get();
            assertThat(kafkaToken.getPositions()).isNotEmpty();
        });

        stream.close();
    }

    private void publishTestEvents(int count) {
        Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerConfig)) {
            for (int i = 0; i < count; i++) {
                String payload = "event-" + i;
                MessageType messageType = new MessageType(String.class);
                EventMessage event = new GenericEventMessage(messageType, payload);
                ProducerRecord<String, byte[]> record = messageConverter.createKafkaMessage(event, TEST_TOPIC);
                producer.send(record);
            }
            producer.flush();
        }
    }
}
