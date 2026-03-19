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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.axonframework.common.Assert.isTrue;
import static org.axonframework.common.Assert.notNull;

/**
 * Utility class for dealing with Kafka {@link Headers}. Mostly for internal use.
 * <p>
 * Adapted for AF5 where metadata values are {@link String} rather than {@link Object}.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 * @since 4.0
 */
public abstract class HeaderUtils {

    private static final Charset UTF_8 = StandardCharsets.UTF_8;
    private static final String KEY_DELIMITER = "-";
    private static final String HEADERS_NULL_MESSAGE = "Headers may not be null";

    private HeaderUtils() {
        // Utility class
    }

    /**
     * Converts bytes to long.
     *
     * @param value the bytes to convert in to a long
     * @return the long build from the given bytes
     */
    public static Long asLong(byte[] value) {
        return value != null ? ByteBuffer.wrap(value).getLong() : null;
    }

    /**
     * Return a {@link Long} representation of the value stored under a given {@code key} inside the
     * {@link Headers}. In case of a missing entry {@code null} is returned.
     *
     * @param headers the Kafka headers to pull the {@link Long} value from
     * @param key     the key corresponding to the expected {@link Long} value
     * @return the value as a {@link Long} corresponding to the given {@code key} in the headers
     */
    public static Long valueAsLong(Headers headers, String key) {
        return asLong(value(headers, key));
    }

    /**
     * Return a {@link Long} representation of the value stored under a given {@code key} inside the
     * {@link Headers}. In case of a missing entry {@code defaultValue} is returned.
     *
     * @param headers      the Kafka headers to pull the {@link Long} value from
     * @param key          the key corresponding to the expected {@link Long} value
     * @param defaultValue the default value to return when {@code key} does not exist
     * @return the value as a {@link Long} corresponding to the given {@code key} in the headers
     */
    public static Long valueAsLong(Headers headers, String key, Long defaultValue) {
        Long val = asLong(value(headers, key));
        return val != null ? val : defaultValue;
    }

    /**
     * Converts the given bytes to {@code int}.
     *
     * @param value the bytes to convert into an {@code int}
     * @return the {@code int} build from the given bytes
     */
    public static Integer asInt(byte[] value) {
        return value != null ? ByteBuffer.wrap(value).getInt() : null;
    }

    /**
     * Return an {@link Integer} representation of the value stored under a given {@code key} inside the
     * {@link Headers}. In case of a missing entry {@code null} is returned.
     *
     * @param headers the Kafka headers to pull the {@link Integer} value from
     * @param key     the key corresponding to the expected {@link Integer} value
     * @return the value as an {@link Integer} corresponding to the given {@code key}
     */
    public static Integer valueAsInt(Headers headers, String key) {
        return asInt(value(headers, key));
    }

    /**
     * Return an {@link Integer} representation of the value stored under a given {@code key} inside the
     * {@link Headers}. In case of a missing entry {@code defaultValue} is returned.
     *
     * @param headers      the Kafka headers to pull the {@link Integer} value from
     * @param key          the key corresponding to the expected {@link Integer} value
     * @param defaultValue the default value to return when {@code key} does not exist
     * @return the value as an {@link Integer} corresponding to the given {@code key}
     */
    public static Integer valueAsInt(Headers headers, String key, Integer defaultValue) {
        Integer val = asInt(value(headers, key));
        return val != null ? val : defaultValue;
    }

    /**
     * Converts bytes to {@link String}.
     *
     * @param value the bytes to convert in to a {@link String}
     * @return the {@link String} build from the given bytes
     */
    public static String asString(byte[] value) {
        return value != null ? new String(value, UTF_8) : null;
    }

    /**
     * Return a {@link String} representation of the value stored under a given {@code key} inside the
     * {@link Headers}. In case of a missing entry {@code null} is returned.
     *
     * @param headers the Kafka headers to pull the {@link String} value from
     * @param key     the key corresponding to the expected {@link String} value
     * @return the value as a {@link String} corresponding to the given {@code key}
     */
    public static String valueAsString(Headers headers, String key) {
        return asString(value(headers, key));
    }

    /**
     * Return a {@link String} representation of the value stored under a given {@code key} inside the
     * {@link Headers}. In case of a missing entry the {@code defaultValue} is returned.
     *
     * @param headers      the Kafka headers to pull the {@link String} value from
     * @param key          the key corresponding to the expected {@link String} value
     * @param defaultValue the default value to return when {@code key} does not exist
     * @return the value as a {@link String} corresponding to the given {@code key}
     */
    public static String valueAsString(Headers headers, String key, String defaultValue) {
        return Objects.toString(asString(value(headers, key)), defaultValue);
    }

    /**
     * Return the value stored under a given {@code key} inside the {@link Headers}. In case of missing entry
     * {@code null} is returned.
     *
     * @param headers the Kafka headers to pull the value from
     * @param key     the key corresponding to the expected value
     * @return the value corresponding to the given {@code key}
     */
    public static byte[] value(Headers headers, String key) {
        isTrue(headers != null, () -> HEADERS_NULL_MESSAGE);
        Header header = headers.lastHeader(key);
        return header != null ? header.value() : null;
    }

    /**
     * Converts primitive arithmetic types to byte array.
     *
     * @param value the {@link Number} to convert into a byte array
     * @return the byte array converted from the given value
     */
    public static byte[] toBytes(Number value) {
        if (value instanceof Short s) {
            return toBytes(s);
        } else if (value instanceof Integer i) {
            return toBytes(i);
        } else if (value instanceof Long l) {
            return toBytes(l);
        } else if (value instanceof Float f) {
            return toBytes(f);
        } else if (value instanceof Double d) {
            return toBytes(d);
        }
        throw new IllegalArgumentException("Cannot convert [" + value + "] to bytes");
    }

    private static byte[] toBytes(Short value) {
        ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES);
        buffer.putShort(value);
        return buffer.array();
    }

    private static byte[] toBytes(Integer value) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(value);
        return buffer.array();
    }

    private static byte[] toBytes(Long value) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(value);
        return buffer.array();
    }

    private static byte[] toBytes(Float value) {
        ByteBuffer buffer = ByteBuffer.allocate(Float.BYTES);
        buffer.putFloat(value);
        return buffer.array();
    }

    private static byte[] toBytes(Double value) {
        ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
        buffer.putDouble(value);
        return buffer.array();
    }

    /**
     * Creates a new {@link RecordHeader} based on {@code key} and {@code value} and adds it to {@code headers}.
     * The value is converted to bytes following this logic:
     * <ul>
     * <li>Instant - calls {@link Instant#toEpochMilli()}</li>
     * <li>Number - calls {@link HeaderUtils#toBytes(Number)}</li>
     * <li>String/custom object - calls {@link Object#toString()}</li>
     * <li>null - {@code null}</li>
     * </ul>
     *
     * @param headers the Kafka headers to add a key/value pair to
     * @param key     the key you want to add to the headers
     * @param value   the value you want to add to the headers
     */
    public static void addHeader(Headers headers, String key, Object value) {
        notNull(headers, () -> "headers may not be null");
        if (value instanceof Instant instant) {
            headers.add(key, toBytes((Number) instant.toEpochMilli()));
        } else if (value instanceof Number number) {
            headers.add(key, toBytes(number));
        } else if (value instanceof String s) {
            headers.add(key, s.getBytes(UTF_8));
        } else if (value == null) {
            headers.add(key, null);
        } else {
            headers.add(key, value.toString().getBytes(UTF_8));
        }
    }

    /**
     * Extract the keys as a {@link Set} from the given headers.
     *
     * @param headers the Kafka headers to extract a key {@link Set} from
     * @return the keys of the given headers
     */
    public static Set<String> keys(Headers headers) {
        notNull(headers, () -> HEADERS_NULL_MESSAGE);

        return StreamSupport.stream(headers.spliterator(), false)
                            .map(Header::key)
                            .collect(Collectors.toSet());
    }

    /**
     * Generates a metadata key used to identify Axon metadata in a {@link RecordHeader}.
     *
     * @param key the key to create an identifiable metadata key out of
     * @return the generated metadata key
     */
    public static String generateMetadataKey(String key) {
        return KafkaHeaders.MESSAGE_METADATA_PREFIX + KEY_DELIMITER + key;
    }

    /**
     * Extracts the actual key name used to send over Axon metadata values.
     * E.g. from {@code 'axon-metadata-foo'} this method will extract {@code foo}.
     *
     * @param metaDataKey the generated metadata key to extract from
     * @return the extracted key
     */
    public static String extractKey(String metaDataKey) {
        isTrue(
                metaDataKey != null && metaDataKey.startsWith(KafkaHeaders.MESSAGE_METADATA_PREFIX + KEY_DELIMITER),
                () -> "Cannot extract Axon MetaData key from given String [" + metaDataKey + "]"
        );

        return metaDataKey.substring((KafkaHeaders.MESSAGE_METADATA_PREFIX + KEY_DELIMITER).length());
    }

    /**
     * Extract all Axon metadata (if any) attached in the given headers.
     * In AF5 metadata values are {@link String}.
     *
     * @param headers the Kafka {@link Headers} to extract metadata from
     * @return the map of all Axon related metadata retrieved from the given headers
     */
    public static Map<String, String> extractAxonMetadata(Headers headers) {
        notNull(headers, () -> HEADERS_NULL_MESSAGE);

        return StreamSupport.stream(headers.spliterator(), false)
                            .filter(header -> isValidMetadataKey(header.key()))
                            .collect(Collectors.toMap(
                                    header -> extractKey(header.key()),
                                    header -> asString(header.value())
                            ));
    }

    private static boolean isValidMetadataKey(String key) {
        return key.startsWith(KafkaHeaders.MESSAGE_METADATA_PREFIX + KEY_DELIMITER);
    }

    /**
     * Generates Kafka {@link Headers} for an {@link EventMessage}, writing standard header fields
     * and metadata entries.
     *
     * @param eventMessage     the {@link EventMessage} to create headers for
     * @param headerValueMapper function for converting values to bytes
     * @return the generated Kafka {@link Headers}
     */
    public static Headers toHeaders(EventMessage eventMessage,
                                    BiFunction<String, Object, RecordHeader> headerValueMapper) {
        notNull(eventMessage, () -> "EventMessage may not be null");
        notNull(headerValueMapper, () -> "Header key-value mapper function may not be null");

        RecordHeaders headers = new RecordHeaders();
        eventMessage.metadata()
                    .forEach((k, v) -> ((Headers) headers).add(headerValueMapper.apply(generateMetadataKey(k), v)));

        addHeader(headers, KafkaHeaders.MESSAGE_ID, eventMessage.identifier());
        addHeader(headers, KafkaHeaders.MESSAGE_TIMESTAMP, eventMessage.timestamp().toEpochMilli());

        return headers;
    }

    /**
     * A {@link BiFunction} that converts values to byte arrays.
     *
     * @return an {@link Object} to {@code byte[]} mapping function
     */
    public static BiFunction<String, Object, RecordHeader> byteMapper() {
        return (key, value) -> value instanceof byte[]
                ? new RecordHeader(key, (byte[]) value)
                : new RecordHeader(key, safeGetBytes(value));
    }

    private static byte[] safeGetBytes(Object input) {
        if (input == null) {
            return null;
        }
        return input.toString().getBytes(UTF_8);
    }
}
