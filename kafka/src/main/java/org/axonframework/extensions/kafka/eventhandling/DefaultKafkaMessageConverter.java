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
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.axonframework.common.BuilderUtils.assertNonNull;
import static org.axonframework.extensions.kafka.eventhandling.HeaderUtils.*;

/**
 * Converts an {@link EventMessage} to a {@link ProducerRecord} Kafka message and from a {@link ConsumerRecord} Kafka
 * message back to an EventMessage (if possible).
 * <p>
 * During conversion metadata entries with the {@code 'axon-metadata-'} prefix are passed to the {@link Headers}. Other
 * message-specific attributes are added as headers. The {@link EventMessage#payload()} is serialized using the
 * configured {@link Converter} and passed as the Kafka record's body.
 * <p>
 * Adapted for AF5 where:
 * <ul>
 *   <li>{@link Converter} replaces Serializer</li>
 *   <li>{@link EventMessage} is not generic</li>
 *   <li>{@link Metadata} values are {@link String}</li>
 *   <li>{@link MessageType} replaces SimpleSerializedType</li>
 * </ul>
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 * @since 4.0
 */
public class DefaultKafkaMessageConverter implements KafkaMessageConverter<String, byte[]> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultKafkaMessageConverter.class);

    private final Converter converter;
    private final Function<EventMessage, Object> keyFunction;
    private final BiFunction<String, Object, RecordHeader> headerValueMapper;

    /**
     * Instantiate a {@link DefaultKafkaMessageConverter} based on the fields contained in the {@link Builder}.
     *
     * @param builder the {@link Builder} used to instantiate a {@link DefaultKafkaMessageConverter} instance
     */
    protected DefaultKafkaMessageConverter(Builder builder) {
        builder.validate();
        this.converter = builder.converter;
        this.keyFunction = builder.keyFunction;
        this.headerValueMapper = builder.headerValueMapper;
    }

    /**
     * Instantiate a Builder to be able to create a {@link DefaultKafkaMessageConverter}.
     * <p>
     * The {@code headerValueMapper} is defaulted to the {@link HeaderUtils#byteMapper()} function, and the
     * {@code keyFunction} defaults to using the aggregate ID from metadata or message identifier.
     * The {@link Converter} is a <b>hard requirement</b> and as such should be provided.
     *
     * @return a Builder to be able to create a {@link DefaultKafkaMessageConverter}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * {@inheritDoc}
     * <p>
     * The {@link ProducerRecord} created through this method sets the timestamp to {@code null}, so the Producer sets
     * one itself. The {@link EventMessage#timestamp()} is still included as a header.
     * <p>
     * The partition is set to {@code null} and the key is defined by the configured key function.
     */
    @Override
    public ProducerRecord<String, byte[]> createKafkaMessage(EventMessage eventMessage, String topic) {
        byte[] payload = converter.convert(eventMessage.payload(), byte[].class);

        Headers headers = buildHeaders(eventMessage);

        return new ProducerRecord<>(
                topic, null, null, recordKey(eventMessage),
                payload,
                headers
        );
    }

    private Headers buildHeaders(EventMessage eventMessage) {
        RecordHeaders headers = new RecordHeaders();

        // Add metadata entries with the axon-metadata prefix
        eventMessage.metadata()
                    .forEach((k, v) -> ((Headers) headers).add(headerValueMapper.apply(generateMetadataKey(k), v)));

        // Add standard Axon headers
        addHeader(headers, KafkaHeaders.MESSAGE_ID, eventMessage.identifier());
        addHeader(headers, KafkaHeaders.MESSAGE_TIMESTAMP, eventMessage.timestamp().toEpochMilli());

        // Payload type from MessageType
        MessageType messageType = eventMessage.type();
        addHeader(headers, KafkaHeaders.MESSAGE_TYPE, messageType.qualifiedName().toString());

        // Revision (version) -- may be the default version
        String version = messageType.version();
        if (version != null && !version.equals(MessageType.DEFAULT_VERSION)) {
            addHeader(headers, KafkaHeaders.MESSAGE_REVISION, version);
        }

        // Aggregate headers from metadata (if present as metadata entries)
        Metadata metadata = eventMessage.metadata();
        String aggregateType = metadata.get("aggregateType");
        if (aggregateType != null) {
            addHeader(headers, KafkaHeaders.AGGREGATE_TYPE, aggregateType);
        }
        String aggregateId = metadata.get("aggregateIdentifier");
        if (aggregateId != null) {
            addHeader(headers, KafkaHeaders.AGGREGATE_ID, aggregateId);
        }
        String aggregateSeq = metadata.get("aggregateSequenceNumber");
        if (aggregateSeq != null) {
            try {
                addHeader(headers, KafkaHeaders.AGGREGATE_SEQ, Long.parseLong(aggregateSeq));
            } catch (NumberFormatException e) {
                addHeader(headers, KafkaHeaders.AGGREGATE_SEQ, aggregateSeq);
            }
        }

        return headers;
    }

    private String recordKey(EventMessage eventMessage) {
        Object key = keyFunction.apply(eventMessage);
        return key != null ? key.toString() : null;
    }

    @Override
    public Optional<EventMessage> readKafkaMessage(ConsumerRecord<String, byte[]> consumerRecord) {
        try {
            Headers headers = consumerRecord.headers();
            if (isAxonMessage(headers)) {
                byte[] messageBody = consumerRecord.value();

                String messageId = valueAsString(headers, KafkaHeaders.MESSAGE_ID);
                String typeName = valueAsString(headers, KafkaHeaders.MESSAGE_TYPE);
                String revision = valueAsString(headers, KafkaHeaders.MESSAGE_REVISION, MessageType.DEFAULT_VERSION);
                Long timestampMillis = valueAsLong(headers, KafkaHeaders.MESSAGE_TIMESTAMP);

                if (messageId == null || typeName == null || timestampMillis == null) {
                    return Optional.empty();
                }

                // Deserialize payload using the Converter
                Object payload = converter.convert(messageBody, Object.class);

                // Build metadata from headers
                Map<String, String> metadataMap = extractAxonMetadata(headers);
                Metadata metadata = Metadata.from(metadataMap);

                MessageType messageType = new MessageType(typeName, revision);
                Instant timestamp = Instant.ofEpochMilli(timestampMillis);

                EventMessage eventMessage = new GenericEventMessage(
                        messageId, messageType, payload, metadata, timestamp
                );
                return Optional.of(eventMessage);
            }
        } catch (Exception e) {
            logger.trace("Error converting ConsumerRecord [{}] to an EventMessage", consumerRecord, e);
        }
        return Optional.empty();
    }

    private static boolean isAxonMessage(Headers headers) {
        return keys(headers).containsAll(Arrays.asList(KafkaHeaders.MESSAGE_ID, KafkaHeaders.MESSAGE_TYPE));
    }

    /**
     * Builder class to instantiate a {@link DefaultKafkaMessageConverter}.
     * <p>
     * The {@code headerValueMapper} is defaulted to the {@link HeaderUtils#byteMapper()} function.
     * The {@code keyFunction} defaults to using the aggregate ID from metadata, falling back to the message identifier.
     * The {@link Converter} is a <b>hard requirement</b> and as such should be provided.
     */
    public static class Builder {

        private Converter converter;
        private BiFunction<String, Object, RecordHeader> headerValueMapper = byteMapper();
        private Function<EventMessage, Object> keyFunction = eventMessage -> {
            String aggregateId = eventMessage.metadata().get("aggregateIdentifier");
            return aggregateId != null ? aggregateId : eventMessage.identifier();
        };

        /**
         * Sets the {@link Converter} to serialize the Event Message's payload with.
         *
         * @param converter The {@link Converter} to serialize the Event Message's payload with
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder converter(Converter converter) {
            assertNonNull(converter, "Converter may not be null");
            this.converter = converter;
            return this;
        }

        /**
         * Sets the {@code headerValueMapper}, a {@link BiFunction} of {@link String}, {@link Object} and
         * {@link RecordHeader}, used for mapping values to Kafka headers. Defaults to the
         * {@link HeaderUtils#byteMapper()} function.
         *
         * @param headerValueMapper a {@link BiFunction} used for mapping values to Kafka headers
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder headerValueMapper(BiFunction<String, Object, RecordHeader> headerValueMapper) {
            assertNonNull(headerValueMapper, "HeaderValueMapper may not be null");
            this.headerValueMapper = headerValueMapper;
            return this;
        }

        /**
         * Sets the function used to determine the record key for a given {@link EventMessage}. Defaults to using the
         * aggregate identifier from metadata, falling back to the message identifier.
         *
         * @param keyFunction a function to derive the record key from an {@link EventMessage}
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder keyFunction(Function<EventMessage, Object> keyFunction) {
            assertNonNull(keyFunction, "KeyFunction may not be null");
            this.keyFunction = keyFunction;
            return this;
        }

        /**
         * Initializes a {@link DefaultKafkaMessageConverter} as specified through this Builder.
         *
         * @return a {@link DefaultKafkaMessageConverter} as specified through this Builder
         */
        public DefaultKafkaMessageConverter build() {
            return new DefaultKafkaMessageConverter(this);
        }

        /**
         * Validates whether the fields contained in this Builder are set accordingly.
         *
         * @throws AxonConfigurationException if one field is asserted to be incorrect
         */
        protected void validate() throws AxonConfigurationException {
            assertNonNull(converter, "The Converter is a hard requirement and should be provided");
        }
    }
}
