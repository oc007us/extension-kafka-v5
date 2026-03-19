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

package org.axonframework.extensions.kafka.eventhandling.consumer.subscribable;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.Registration;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.Fetcher;
import org.axonframework.extensions.kafka.eventhandling.consumer.RuntimeErrorHandler;
import org.axonframework.extensions.kafka.eventhandling.consumer.TopicSubscriber;
import org.axonframework.extensions.kafka.eventhandling.consumer.TopicSubscriberBuilder;
import org.axonframework.messaging.core.SubscribableEventSource;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.axonframework.common.BuilderUtils.assertNonNull;
import static org.axonframework.common.BuilderUtils.assertThat;

/**
 * A {@link SubscribableEventSource} implementation using Kafka's {@link Consumer} API to poll
 * {@link ConsumerRecords}. These records will be converted with a {@link KafkaMessageConverter} and given to the Event
 * Processors subscribed to this source through the {@link #subscribe} method.
 * <p>
 * This approach allows the usage of Kafka's idea of load partitioning (through several Consumer instances in a Consumer
 * Group) and resetting by adjusting a Consumer's offset. To share the load of a topic within a single application, the
 * {@link Builder#consumerCount(int)} can be increased to have multiple Consumer instances for this source, as each is
 * connected to the same Consumer Group.
 * <p>
 * If Axon's approach of segregating the event stream and replaying is desired, use the
 * {@link org.axonframework.extensions.kafka.eventhandling.consumer.streamable.StreamableKafkaEventSource} instead.
 *
 * @param <K> the key of the {@link ConsumerRecords} to consume, fetch and convert
 * @param <V> the value type of {@link ConsumerRecords} to consume, fetch and convert
 * @author Steven van Beelen
 * @since 5.0
 */
public class SubscribableKafkaEventSource<K, V> implements SubscribableEventSource {

    private static final Logger logger = LoggerFactory.getLogger(SubscribableKafkaEventSource.class);

    private final TopicSubscriber subscriber;
    private final String groupId;
    private final ConsumerFactory<K, V> consumerFactory;
    private final Fetcher<K, V, EventMessage> fetcher;
    private final KafkaMessageConverter<K, V> messageConverter;
    private final boolean autoStart;
    private final int consumerCount;

    private final Set<BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>>>
            eventProcessors = new CopyOnWriteArraySet<>();
    private final Map<Integer, Registration> fetcherRegistrations = new ConcurrentHashMap<>();
    private final AtomicBoolean inProgress = new AtomicBoolean(false);

    /**
     * Instantiate a {@link SubscribableKafkaEventSource} based on the fields contained in the {@link Builder}.
     *
     * @param builder the {@link Builder} used to instantiate a {@link SubscribableKafkaEventSource} instance
     */
    protected SubscribableKafkaEventSource(Builder<K, V> builder) {
        builder.validate();
        this.subscriber = builder.getSubscriber();
        this.groupId = builder.groupId;
        this.consumerFactory = builder.consumerFactory;
        this.fetcher = builder.fetcher;
        this.messageConverter = builder.messageConverter;
        this.autoStart = builder.autoStart;
        this.consumerCount = builder.consumerCount;
    }

    /**
     * Instantiate a Builder to be able to create a {@link SubscribableKafkaEventSource}.
     * <p>
     * The topics subscribed to is defaulted to {@code "Axon.Events"}. The {@code groupId}, {@link ConsumerFactory},
     * {@link Fetcher}, and {@link KafkaMessageConverter} are <b>hard requirements</b> and as such should be provided.
     *
     * @param <K> the key of the {@link ConsumerRecords} to consume, fetch and convert
     * @param <V> the value type of {@link ConsumerRecords} to consume, fetch and convert
     * @return a Builder to be able to create a {@link SubscribableKafkaEventSource}
     */
    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Any subscribed Event Processor will be placed in the same Consumer Group, defined through the (mandatory)
     * {@link Builder#groupId(String)} method.
     */
    @Override
    public Registration subscribe(
            BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> eventProcessor
    ) {
        if (this.eventProcessors.add(eventProcessor)) {
            logger.debug("Event Processor [{}] subscribed successfully", eventProcessor);
        } else {
            logger.info("Event Processor [{}] not added. It was already subscribed", eventProcessor);
        }

        if (autoStart) {
            logger.info("Starting event consumption as auto start is enabled");
            start();
        }

        return () -> {
            if (eventProcessors.remove(eventProcessor)) {
                logger.debug("Event Processor [{}] unsubscribed successfully", eventProcessor);
                if (eventProcessors.isEmpty() && autoStart) {
                    logger.info("Closing event consumption as auto start is enabled");
                    close();
                }
                return true;
            } else {
                logger.info("Event Processor [{}] not removed. It was already unsubscribed", eventProcessor);
                return false;
            }
        };
    }

    /**
     * Start polling the configured topics with a {@link Consumer} built by the {@link ConsumerFactory}.
     * <p>
     * This operation should be called <b>only</b> if all desired Event Processors have been subscribed (through the
     * {@link #subscribe} method).
     */
    public void start() {
        if (inProgress.getAndSet(true)) {
            return;
        }

        for (int consumerIndex = 0; consumerIndex < consumerCount; consumerIndex++) {
            addConsumer(consumerIndex);
        }
    }

    private void addConsumer(int consumerIndex) {
        Consumer<K, V> consumer = consumerFactory.createConsumer(groupId);
        subscriber.subscribeTopics(consumer);

        Registration closeConsumer = fetcher.poll(
                consumer,
                consumerRecords -> StreamSupport.stream(consumerRecords.spliterator(), false)
                                                .map(messageConverter::readKafkaMessage)
                                                .filter(Optional::isPresent)
                                                .map(Optional::get)
                                                .collect(Collectors.toList()),
                eventMessages -> eventProcessors.forEach(
                        eventProcessor -> eventProcessor.apply(eventMessages, null)
                ),
                restartOnError(consumerIndex)
        );
        fetcherRegistrations.put(consumerIndex, closeConsumer);
    }

    private RuntimeErrorHandler restartOnError(int consumerIndex) {
        return e -> {
            logger.warn("Consumer had a fatal exception, starting a new one", e);
            addConsumer(consumerIndex);
        };
    }

    /**
     * Close off this {@link SubscribableKafkaEventSource} ensuring all used {@link Consumer}s are closed.
     */
    public void close() {
        if (fetcherRegistrations.isEmpty()) {
            logger.debug("No Event Processors have been subscribed whose Consumers should be closed");
            return;
        }
        fetcherRegistrations.values().forEach(Registration::cancel);
        fetcherRegistrations.clear();
        inProgress.set(false);
    }

    /**
     * Builder class to instantiate a {@link SubscribableKafkaEventSource}.
     * <p>
     * The topics subscribed to is defaulted to {@code "Axon.Events"}. The {@code groupId}, {@link ConsumerFactory},
     * {@link Fetcher}, and {@link KafkaMessageConverter} are <b>hard requirements</b> and as such should be provided.
     *
     * @param <K> the key of the {@link ConsumerRecords} to consume, fetch and convert
     * @param <V> the value type of {@link ConsumerRecords} to consume, fetch and convert
     */
    public static class Builder<K, V> extends TopicSubscriberBuilder<Builder<K, V>> {

        private String groupId;
        private ConsumerFactory<K, V> consumerFactory;
        private Fetcher<K, V, EventMessage> fetcher;
        private KafkaMessageConverter<K, V> messageConverter;
        private boolean autoStart = false;
        private int consumerCount = 1;

        @Override
        protected Builder<K, V> self() {
            return this;
        }

        /**
         * Sets the Consumer {@code groupId} from which a {@link Consumer} should retrieve records from.
         *
         * @param groupId a {@link String} defining the Consumer Group id
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> groupId(String groupId) {
            assertThat(groupId, name -> Objects.nonNull(name) && !name.isEmpty(),
                       "The groupId may not be null or empty");
            this.groupId = groupId;
            return this;
        }

        /**
         * Sets the {@link ConsumerFactory} to be used to create {@link Consumer} instances.
         *
         * @param consumerFactory a {@link ConsumerFactory} to create {@link Consumer} instances
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
        public Builder<K, V> fetcher(Fetcher<K, V, EventMessage> fetcher) {
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
         * Toggles on auto-start behavior: after the first {@link #subscribe} operation, the {@link #start()} method
         * of this source will be called. Once all registered consumers are removed, this setting will close the source.
         * By default this behavior is turned off.
         *
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> autoStart() {
            autoStart = true;
            return this;
        }

        /**
         * Sets the number of {@link Consumer} instances to create when this source starts consuming events.
         * Defaults to {@code 1}.
         *
         * @param consumerCount the number of {@link Consumer} instances to create
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> consumerCount(int consumerCount) {
            assertThat(consumerCount, count -> count > 0, "The consumer count must be a positive, non-zero number");
            this.consumerCount = consumerCount;
            return this;
        }

        /**
         * Initializes a {@link SubscribableKafkaEventSource} as specified through this Builder.
         *
         * @return a {@link SubscribableKafkaEventSource} as specified through this Builder
         */
        public SubscribableKafkaEventSource<K, V> build() {
            return new SubscribableKafkaEventSource<>(this);
        }

        /**
         * Validates whether the fields contained in this Builder are set accordingly.
         *
         * @throws AxonConfigurationException if one field is asserted to be incorrect
         */
        protected void validate() throws AxonConfigurationException {
            assertNonNull(groupId, "The Consumer Group Id is a hard requirement and should be provided");
            assertNonNull(consumerFactory, "The ConsumerFactory is a hard requirement and should be provided");
            assertNonNull(fetcher, "The Fetcher is a hard requirement and should be provided");
            assertNonNull(messageConverter, "The KafkaMessageConverter is a hard requirement and should be provided");
        }
    }
}
