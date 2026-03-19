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

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;

import static org.axonframework.common.BuilderUtils.assertNonEmpty;
import static org.axonframework.common.BuilderUtils.assertNonNull;

/**
 * A simple event handler responsible for sending {@link EventMessage} instances to Kafka using the provided
 * {@link KafkaPublisher}.
 * <p>
 * Adapted for AF5: provides a {@code handle(EventMessage, ProcessingContext)} method instead of implementing the
 * AF4 {@code EventMessageHandler} interface. The processing context is passed explicitly.
 *
 * @param <K> a generic type for the key of the {@link KafkaPublisher}
 * @param <V> a generic type for the value of the {@link KafkaPublisher}
 * @author Simon Zambrovski
 * @author Steven van Beelen
 * @since 4.0
 */
public class KafkaEventPublisher<K, V> {

    /**
     * The default Kafka Event Handler processing group. Not intended to be used by other Event Handling Components than
     * {@link KafkaEventPublisher}.
     */
    public static final String DEFAULT_PROCESSING_GROUP = "__axon-kafka-event-publishing-group";

    private final String processingGroup;
    private final KafkaPublisher<K, V> kafkaPublisher;

    /**
     * Instantiate a Builder to be able to create a {@link KafkaEventPublisher}.
     * <p>
     * The {@link KafkaPublisher} is a <b>hard requirement</b> and as such should be provided.
     *
     * @param <K> a generic type for the key of the {@link KafkaPublisher}
     * @param <V> a generic type for the value of the {@link KafkaPublisher}
     * @return a Builder to be able to create a {@link KafkaEventPublisher}
     */
    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    /**
     * Instantiate a {@link KafkaEventPublisher} based on the fields contained in the {@link Builder}.
     *
     * @param builder the {@link Builder} used to instantiate a {@link KafkaEventPublisher} instance
     */
    protected KafkaEventPublisher(Builder<K, V> builder) {
        builder.validate();
        this.processingGroup = builder.processingGroup;
        this.kafkaPublisher = builder.kafkaPublisher;
    }

    /**
     * Handle the given {@link EventMessage} by publishing it to Kafka through the configured {@link KafkaPublisher}.
     *
     * @param event   the {@link EventMessage} to publish
     * @param context the {@link ProcessingContext}, or {@code null} if no processing context is active
     */
    public void handle(EventMessage event, @Nullable ProcessingContext context) {
        kafkaPublisher.send(event, context);
    }

    /**
     * Returns the configured Processing Group name.
     *
     * @return the Processing Group name
     */
    public String getProcessingGroup() {
        return this.processingGroup;
    }

    /**
     * Builder class to instantiate a {@link KafkaEventPublisher}.
     * <p>
     * The {@link KafkaPublisher} is a <b>hard requirement</b> and as such should be provided.
     *
     * @param <K> a generic type for the key of the {@link KafkaPublisher}
     * @param <V> a generic type for the value of the {@link KafkaPublisher}
     */
    public static class Builder<K, V> {

        private String processingGroup = DEFAULT_PROCESSING_GROUP;
        private KafkaPublisher<K, V> kafkaPublisher;

        /**
         * Sets the Kafka Event Handler processing group to be used by this publisher.
         *
         * @param processingGroup the processing group name
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> processingGroup(String processingGroup) {
            assertNonEmpty(processingGroup, "ProcessingGroup may not be null or empty");
            this.processingGroup = processingGroup;
            return this;
        }

        /**
         * Sets the {@link KafkaPublisher} to be used to publish {@link EventMessage}s.
         *
         * @param kafkaPublisher the {@link KafkaPublisher}
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> kafkaPublisher(KafkaPublisher<K, V> kafkaPublisher) {
            assertNonNull(kafkaPublisher, "KafkaPublisher may not be null");
            this.kafkaPublisher = kafkaPublisher;
            return this;
        }

        /**
         * Initializes a {@link KafkaEventPublisher} as specified through this Builder.
         *
         * @return a {@link KafkaEventPublisher} as specified through this Builder
         */
        public KafkaEventPublisher<K, V> build() {
            return new KafkaEventPublisher<>(this);
        }

        /**
         * Validates whether the fields contained in this Builder are set accordingly.
         *
         * @throws AxonConfigurationException if one field is asserted to be incorrect
         */
        protected void validate() {
            assertNonNull(kafkaPublisher, "The KafkaPublisher is a hard requirement and should be provided");
        }
    }
}
