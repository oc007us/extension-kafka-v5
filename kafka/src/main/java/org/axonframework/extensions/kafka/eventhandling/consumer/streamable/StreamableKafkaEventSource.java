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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.Registration;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerSeekUtil;
import org.axonframework.extensions.kafka.eventhandling.consumer.Fetcher;
import org.axonframework.extensions.kafka.eventhandling.consumer.TopicSubscriber;
import org.axonframework.extensions.kafka.eventhandling.consumer.TopicSubscriberBuilder;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.StreamableEventSource;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.axonframework.common.BuilderUtils.assertNonNull;

/**
 * Implementation of the {@link StreamableEventSource} that reads messages from a Kafka topic using the provided
 * {@link Fetcher}. Will create new {@link Consumer} instances for every call of
 * {@link #open(StreamingCondition, ProcessingContext)}, for which it will create a unique Consumer Group Id.
 * <p>
 * The latter ensures that we can guarantee that each Consumer Group receives all messages, so that the event processor
 * and its sequencing policy are in charge of partitioning the load instead of Kafka.
 *
 * @param <K> the key of the {@link ConsumerRecords} to consume, fetch and convert
 * @param <V> the value type of {@link ConsumerRecords} to consume, fetch and convert
 * @author Nakul Mishra
 * @author Steven van Beelen
 * @author Gerard Klijs
 * @since 5.0
 */
public class StreamableKafkaEventSource<K, V> implements StreamableEventSource {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final TopicSubscriber subscriber;
    private final ConsumerFactory<K, V> consumerFactory;
    private final Fetcher<K, V, KafkaEventMessage> fetcher;
    private final KafkaMessageConverter<K, V> messageConverter;
    private final Supplier<Buffer<KafkaEventMessage>> bufferFactory;

    /**
     * Instantiate a {@link StreamableKafkaEventSource} based on the fields contained in the {@link Builder}.
     *
     * @param builder the {@link Builder} used to instantiate a {@link StreamableKafkaEventSource} instance
     */
    protected StreamableKafkaEventSource(Builder<K, V> builder) {
        builder.validate();
        this.subscriber = builder.getSubscriber();
        this.consumerFactory = builder.consumerFactory;
        this.fetcher = builder.fetcher;
        this.messageConverter = builder.messageConverter;
        this.bufferFactory = builder.bufferFactory;
    }

    /**
     * Instantiate a Builder to be able to create a {@link StreamableKafkaEventSource}.
     * <p>
     * The topics subscribed to is defaulted to {@code "Axon.Events"} and the {@code bufferFactory} to the
     * {@link SortedKafkaMessageBuffer} constructor. The {@link ConsumerFactory}, {@link Fetcher}, and
     * {@link KafkaMessageConverter} are <b>hard requirements</b> and as such should be provided.
     *
     * @param <K> the key of the {@link ConsumerRecords} to consume, fetch and convert
     * @param <V> the value type of {@link ConsumerRecords} to consume, fetch and convert
     * @return a Builder to be able to create a {@link StreamableKafkaEventSource}
     */
    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Opens a stream filled by polling {@link ConsumerRecords} from the specified topics with the {@link Fetcher}.
     * The position is extracted from the given {@link StreamingCondition}; any criteria/tags are ignored since Kafka
     * does not support AF5 tag filtering natively.
     */
    @Override
    public MessageStream<EventMessage> open(@Nonnull StreamingCondition condition,
                                            @Nullable ProcessingContext context) {
        TrackingToken position = condition.position();
        KafkaTrackingToken token = KafkaTrackingToken.from(position);
        TrackingRecordConverter<K, V> recordConverter = new TrackingRecordConverter<>(messageConverter, token);

        logger.debug("Opening stream from topics: {}", subscriber.describe());
        Consumer<K, V> consumer = consumerFactory.createConsumer(null);
        ConsumerSeekUtil.seekToCurrentPositions(consumer, recordConverter::currentToken, subscriber);

        Buffer<KafkaEventMessage> buffer = bufferFactory.get();
        // We need a reference to the stream for error reporting, but also need the fetcher registration
        // for close handling. Use a holder to break the circular dependency.
        KafkaMessageStream[] streamHolder = new KafkaMessageStream[1];

        Registration fetcherRegistration = fetcher.poll(
                consumer,
                recordConverter,
                buffer::putAll,
                exception -> {
                    buffer.setException(exception);
                    KafkaMessageStream s = streamHolder[0];
                    if (s != null) {
                        s.reportError(exception);
                    }
                }
        );

        KafkaMessageStream stream = new KafkaMessageStream(buffer, () -> fetcherRegistration.cancel());
        streamHolder[0] = stream;
        return stream;
    }

    @Override
    public CompletableFuture<TrackingToken> firstToken(@Nullable ProcessingContext context) {
        return CompletableFuture.supplyAsync(() -> KafkaTrackingToken.emptyToken());
    }

    @Override
    public CompletableFuture<TrackingToken> latestToken(@Nullable ProcessingContext context) {
        return CompletableFuture.supplyAsync(() -> {
            Consumer<K, V> consumer = consumerFactory.createConsumer(null);
            try {
                return KafkaTrackingToken.newInstance(
                        ConsumerPositionsUtil.getHeadPositions(consumer, subscriber)
                );
            } finally {
                consumer.close();
            }
        });
    }

    @Override
    public CompletableFuture<TrackingToken> tokenAt(@Nonnull Instant at, @Nullable ProcessingContext context) {
        return CompletableFuture.supplyAsync(() -> {
            Consumer<K, V> consumer = consumerFactory.createConsumer(null);
            try {
                return KafkaTrackingToken.newInstance(
                        ConsumerPositionsUtil.getPositionsBasedOnTime(consumer, subscriber, at)
                );
            } finally {
                consumer.close();
            }
        });
    }

    /**
     * Builder class to instantiate a {@link StreamableKafkaEventSource}.
     * <p>
     * The topics subscribed to is defaulted to {@code "Axon.Events"} and the {@code bufferFactory} to the
     * {@link SortedKafkaMessageBuffer} constructor. The {@link ConsumerFactory}, {@link Fetcher}, and
     * {@link KafkaMessageConverter} are <b>hard requirements</b> and as such should be provided.
     *
     * @param <K> the key of the {@link ConsumerRecords} to consume, fetch and convert
     * @param <V> the value type of {@link ConsumerRecords} to consume, fetch and convert
     */
    public static class Builder<K, V> extends TopicSubscriberBuilder<Builder<K, V>> {

        private ConsumerFactory<K, V> consumerFactory;
        private Fetcher<K, V, KafkaEventMessage> fetcher;
        private KafkaMessageConverter<K, V> messageConverter;
        private Supplier<Buffer<KafkaEventMessage>> bufferFactory = SortedKafkaMessageBuffer::new;

        @Override
        protected Builder<K, V> self() {
            return this;
        }

        /**
         * Sets the {@link ConsumerFactory} to be used by this {@link StreamableKafkaEventSource} to create
         * {@link Consumer} instances with.
         *
         * @param consumerFactory a {@link ConsumerFactory} to create {@link Consumer} instances with
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> consumerFactory(ConsumerFactory<K, V> consumerFactory) {
            assertNonNull(consumerFactory, "ConsumerFactory may not be null");
            this.consumerFactory = consumerFactory;
            return this;
        }

        /**
         * Sets the {@link Fetcher} used to poll, convert and consume {@link ConsumerRecords} with.
         *
         * @param fetcher the {@link Fetcher} used to poll, convert and consume {@link ConsumerRecords} with
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> fetcher(Fetcher<K, V, KafkaEventMessage> fetcher) {
            assertNonNull(fetcher, "Fetcher may not be null");
            this.fetcher = fetcher;
            return this;
        }

        /**
         * Sets the {@link KafkaMessageConverter} used to convert Kafka messages into {@link EventMessage}s.
         *
         * @param messageConverter a {@link KafkaMessageConverter} used to convert Kafka messages into
         *                         {@link EventMessage}s
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> messageConverter(KafkaMessageConverter<K, V> messageConverter) {
            assertNonNull(messageConverter, "MessageConverter may not be null");
            this.messageConverter = messageConverter;
            return this;
        }

        /**
         * Sets the {@code bufferFactory} of type {@link Supplier} with a generic type {@link Buffer} with
         * {@link KafkaEventMessage}s. Used to create a buffer which will consume the converted Kafka
         * {@link ConsumerRecords}. Defaults to a {@link SortedKafkaMessageBuffer}.
         *
         * @param bufferFactory a {@link Supplier} to create a buffer for the Kafka records fetcher
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> bufferFactory(Supplier<Buffer<KafkaEventMessage>> bufferFactory) {
            assertNonNull(bufferFactory, "Buffer factory may not be null");
            this.bufferFactory = bufferFactory;
            return this;
        }

        /**
         * Initializes a {@link StreamableKafkaEventSource} as specified through this Builder.
         *
         * @return a {@link StreamableKafkaEventSource} as specified through this Builder
         */
        public StreamableKafkaEventSource<K, V> build() {
            return new StreamableKafkaEventSource<>(this);
        }

        /**
         * Validates whether the fields contained in this Builder are set accordingly.
         *
         * @throws AxonConfigurationException if one field is asserted to be incorrect
         */
        protected void validate() throws AxonConfigurationException {
            assertNonNull(consumerFactory, "The ConsumerFactory is a hard requirement and should be provided");
            assertNonNull(fetcher, "The Fetcher is a hard requirement and should be provided");
            assertNonNull(messageConverter, "The KafkaMessageConverter is a hard requirement and should be provided");
        }
    }
}
