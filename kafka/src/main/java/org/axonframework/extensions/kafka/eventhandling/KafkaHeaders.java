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

/**
 * Constants for Kafka record header keys used to carry Axon message attributes.
 * <p>
 * Replaces the AF4 {@code org.axonframework.messaging.Headers} constants with
 * Kafka-extension-local equivalents for AF5.
 *
 * @author Axon Framework Contributors
 * @since 5.0
 */
public final class KafkaHeaders {

    /**
     * Header key for the unique message identifier.
     */
    public static final String MESSAGE_ID = "axon-message-id";

    /**
     * Header key for the fully qualified payload type name.
     */
    public static final String MESSAGE_TYPE = "axon-message-type";

    /**
     * Header key for the message timestamp (epoch millis).
     */
    public static final String MESSAGE_TIMESTAMP = "axon-message-timestamp";

    /**
     * Header key for the message/payload revision.
     */
    public static final String MESSAGE_REVISION = "axon-message-revision";

    /**
     * Header key for the aggregate type (present only for domain events).
     */
    public static final String AGGREGATE_TYPE = "axon-message-aggregate-type";

    /**
     * Header key for the aggregate identifier (present only for domain events).
     */
    public static final String AGGREGATE_ID = "axon-message-aggregate-id";

    /**
     * Header key for the aggregate sequence number (present only for domain events).
     */
    public static final String AGGREGATE_SEQ = "axon-message-aggregate-seq";

    /**
     * Prefix used for metadata entries stored as Kafka headers.
     */
    public static final String MESSAGE_METADATA_PREFIX = "axon-metadata";

    private KafkaHeaders() {
        // Utility class
    }
}
