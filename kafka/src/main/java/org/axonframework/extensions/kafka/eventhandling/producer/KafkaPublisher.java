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
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.axonframework.common.BuilderUtils.assertNonNull;
import static org.axonframework.common.BuilderUtils.assertThat;

/**
 * Publisher implementation that uses Kafka Message Broker to dispatch event messages. All outgoing messages are sent to
 * configured topics.
 * <p>
 * Adapted for AF5: accepts an explicit {@link ProcessingContext} parameter instead of using
 * {@code CurrentUnitOfWork}. Does not implement the Lifecycle interface.
 *
 * @param <K> a generic type for the key of the {@link ProducerFactory}, {@link Producer} and
 *            {@link KafkaMessageConverter}
 * @param <V> a generic type for the value of the {@link ProducerFactory}, {@link Producer} and
 *            {@link KafkaMessageConverter}
 * @author Nakul Mishra
 * @author Simon Zambrovski
 * @author Lars Bilger
 * @since 4.0
 */
public class KafkaPublisher<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaPublisher.class);

    private static final String DEFAULT_TOPIC = "Axon.Events";

    private final ProducerFactory<K, V> producerFactory;
    private final KafkaMessageConverter<K, V> messageConverter;
    private final TopicResolver topicResolver;
    private final long publisherAckTimeout;

    /**
     * Instantiate a {@link KafkaPublisher} based on the fields contained in the {@link Builder}.
     *
     * @param builder the {@link Builder} used to instantiate a {@link KafkaPublisher} instance
     */
    protected KafkaPublisher(Builder<K, V> builder) {
        builder.validate();
        this.producerFactory = builder.producerFactory;
        this.messageConverter = builder.messageConverter;
        this.topicResolver = builder.topicResolver;
        this.publisherAckTimeout = builder.publisherAckTimeout;
    }

    /**
     * Instantiate a Builder to be able to create a {@link KafkaPublisher}.
     *
     * @param <K> a generic type for the key
     * @param <V> a generic type for the value
     * @return a Builder to be able to create a {@link KafkaPublisher}
     */
    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    /**
     * Send the given {@code event} to the configured Kafka topic. If a {@link ProcessingContext} is provided, a
     * rollback handler is registered to abort any in-flight transactions on error.
     *
     * @param event   the event to publish on the Kafka broker
     * @param context the processing context, or {@code null} if no processing context is active
     */
    public void send(EventMessage event, @Nullable ProcessingContext context) {
        logger.debug("Starting event producing process for [{}].", event.payloadType());
        Optional<String> topic = topicResolver.resolve(event);
        if (topic.isEmpty()) {
            logger.debug("Skip publishing event for [{}] since topicFunction returned empty.", event.payloadType());
            return;
        }

        Producer<K, V> producer = producerFactory.createProducer();
        ConfirmationMode confirmationMode = producerFactory.confirmationMode();

        if (confirmationMode.isTransactional()) {
            tryBeginTxn(producer);
        }

        // Sends event messages to Kafka and receive a future indicating the status
        Future<RecordMetadata> publishStatus = producer.send(messageConverter.createKafkaMessage(event, topic.get()));

        if (context != null) {
            context.onError((ctx, phase, throwable) -> {
                // Provides a way to prevent duplicate messages on Kafka, in case there is a problem with the token store
                if (confirmationMode.isTransactional()) {
                    tryRollback(producer);
                }
                tryClose(producer);
            });
        }

        if (confirmationMode.isTransactional()) {
            tryCommit(producer);
        } else if (confirmationMode.isWaitForAck()) {
            waitForPublishAck(publishStatus);
        }
        tryClose(producer);
    }

    private void tryBeginTxn(Producer<?, ?> producer) {
        try {
            producer.beginTransaction();
        } catch (ProducerFencedException e) {
            logger.warn("Unable to begin transaction");
            throw new RuntimeException(
                    "Event publication failed, exception occurred while starting Kafka transaction.", e
            );
        }
    }

    private void tryCommit(Producer<?, ?> producer) {
        try {
            producer.commitTransaction();
        } catch (ProducerFencedException e) {
            logger.warn("Unable to commit transaction");
            throw new RuntimeException(
                    "Event publication failed, exception occurred while committing Kafka transaction.", e
            );
        }
    }

    private void waitForPublishAck(Future<RecordMetadata> future) {
        long deadline = System.currentTimeMillis() + publisherAckTimeout;
        try {
            future.get(Math.max(0, deadline - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.warn("Encountered error while waiting for event publication.", e);
            Thread.currentThread().interrupt();
        } catch (ExecutionException | TimeoutException e) {
            logger.warn("Encountered error while waiting for event publication.");
            throw new RuntimeException(
                    "Event publication failed, exception occurred while waiting for event publication.", e
            );
        }
    }

    private void tryRollback(Producer<?, ?> producer) {
        try {
            producer.abortTransaction();
        } catch (Exception e) {
            logger.warn("Unable to abort transaction.", e);
            // Not re-throwing exception, it's too late.
        }
    }

    private void tryClose(Producer<?, ?> producer) {
        try {
            producer.close();
        } catch (Exception e) {
            logger.debug("Unable to close producer.", e);
            // Not re-throwing exception, can't do anything.
        }
    }

    /**
     * Shuts down this component by calling {@link ProducerFactory#shutDown()} ensuring no new {@link Producer}
     * instances can be created.
     */
    public void shutDown() {
        producerFactory.shutDown();
    }

    /**
     * Builder class to instantiate a {@link KafkaPublisher}.
     * <p>
     * The {@link KafkaMessageConverter} is a <b>hard requirement</b>, the topic defaults to {@code Axon.Events},
     * and the {@code publisherAckTimeout} defaults to {@code 1000} milliseconds.
     * The {@link ProducerFactory} is a <b>hard requirement</b> and as such should be provided.
     *
     * @param <K> a generic type for the key
     * @param <V> a generic type for the value
     */
    public static class Builder<K, V> {

        private ProducerFactory<K, V> producerFactory;
        private KafkaMessageConverter<K, V> messageConverter;
        private TopicResolver topicResolver = m -> Optional.of(DEFAULT_TOPIC);
        private long publisherAckTimeout = 1_000;

        /**
         * Sets the {@link ProducerFactory} which will instantiate {@link Producer} instances to publish
         * {@link EventMessage}s on the Kafka topic.
         *
         * @param producerFactory a {@link ProducerFactory}
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> producerFactory(ProducerFactory<K, V> producerFactory) {
            assertNonNull(producerFactory, "ProducerFactory may not be null");
            this.producerFactory = producerFactory;
            return this;
        }

        /**
         * Sets the {@link KafkaMessageConverter} used to convert {@link EventMessage}s into Kafka messages.
         *
         * @param messageConverter a {@link KafkaMessageConverter}
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> messageConverter(KafkaMessageConverter<K, V> messageConverter) {
            assertNonNull(messageConverter, "MessageConverter may not be null");
            this.messageConverter = messageConverter;
            return this;
        }

        /**
         * Set the Kafka {@code topic} to publish {@link EventMessage}s on. Defaults to {@code Axon.Events}.
         *
         * @param topic the Kafka topic
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> topic(String topic) {
            assertThat(topic, name -> name != null && !name.isEmpty(), "The topic may not be null or empty");
            this.topicResolver = m -> Optional.of(topic);
            return this;
        }

        /**
         * Set the resolver to determine the Kafka topic to publish a certain {@link EventMessage} to.
         * Defaults to always return {@code Axon.Events}.
         *
         * @param topicResolver the topic resolver
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> topicResolver(TopicResolver topicResolver) {
            assertNonNull(topicResolver, "The TopicResolver may not be null");
            this.topicResolver = topicResolver;
            return this;
        }

        /**
         * Sets the publisher acknowledge timeout in milliseconds specifying how long to wait for a publisher to
         * acknowledge a message has been sent. Defaults to {@code 1000} milliseconds.
         *
         * @param publisherAckTimeout a {@code long} specifying the timeout
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> publisherAckTimeout(long publisherAckTimeout) {
            assertThat(publisherAckTimeout, timeout -> timeout >= 0,
                    "The publisherAckTimeout should be a positive number or zero");
            this.publisherAckTimeout = publisherAckTimeout;
            return this;
        }

        /**
         * Initializes a {@link KafkaPublisher} as specified through this Builder.
         *
         * @return a {@link KafkaPublisher} as specified through this Builder
         */
        public KafkaPublisher<K, V> build() {
            return new KafkaPublisher<>(this);
        }

        /**
         * Validates whether the fields contained in this Builder are set accordingly.
         *
         * @throws AxonConfigurationException if one field is asserted to be incorrect
         */
        protected void validate() throws AxonConfigurationException {
            assertNonNull(producerFactory, "The ProducerFactory is a hard requirement and should be provided");
            assertNonNull(messageConverter, "The KafkaMessageConverter is a hard requirement and should be provided");
        }
    }
}
