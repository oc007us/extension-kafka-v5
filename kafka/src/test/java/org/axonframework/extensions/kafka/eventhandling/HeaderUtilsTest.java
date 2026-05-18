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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HeaderUtilsTest {

    @Test
    void addAndReadStringHeader() {
        RecordHeaders headers = new RecordHeaders();
        HeaderUtils.addHeader(headers, "test-key", "test-value");

        assertThat(HeaderUtils.valueAsString(headers, "test-key")).isEqualTo("test-value");
    }

    @Test
    void addAndReadLongHeader() {
        RecordHeaders headers = new RecordHeaders();
        HeaderUtils.addHeader(headers, "test-long", 42L);

        assertThat(HeaderUtils.valueAsLong(headers, "test-long")).isEqualTo(42L);
    }

    @Test
    void addAndReadIntHeader() {
        RecordHeaders headers = new RecordHeaders();
        byte[] intBytes = ByteBuffer.allocate(Integer.BYTES).putInt(99).array();
        headers.add("test-int", intBytes);

        assertThat(HeaderUtils.valueAsInt(headers, "test-int")).isEqualTo(99);
    }

    @Test
    void valueAsLongReturnsDefaultWhenMissing() {
        RecordHeaders headers = new RecordHeaders();
        assertThat(HeaderUtils.valueAsLong(headers, "missing", 7L)).isEqualTo(7L);
    }

    @Test
    void valueAsIntReturnsDefaultWhenMissing() {
        RecordHeaders headers = new RecordHeaders();
        assertThat(HeaderUtils.valueAsInt(headers, "missing", 3)).isEqualTo(3);
    }

    @Test
    void valueAsStringReturnsDefaultWhenMissing() {
        RecordHeaders headers = new RecordHeaders();
        assertThat(HeaderUtils.valueAsString(headers, "missing", "fallback")).isEqualTo("fallback");
    }

    @Test
    void valueReturnsNullForMissingKey() {
        RecordHeaders headers = new RecordHeaders();
        assertThat(HeaderUtils.value(headers, "non-existent")).isNull();
    }

    @Test
    void addNullValueHeader() {
        RecordHeaders headers = new RecordHeaders();
        HeaderUtils.addHeader(headers, "null-key", null);

        assertThat(HeaderUtils.value(headers, "null-key")).isNull();
    }

    @Test
    void keysReturnsAllHeaderKeys() {
        RecordHeaders headers = new RecordHeaders();
        headers.add("key-a", "val".getBytes(StandardCharsets.UTF_8));
        headers.add("key-b", "val".getBytes(StandardCharsets.UTF_8));

        Set<String> keys = HeaderUtils.keys(headers);
        assertThat(keys).containsExactlyInAnyOrder("key-a", "key-b");
    }

    @Test
    void generateAndExtractMetadataKey() {
        String generated = HeaderUtils.generateMetadataKey("myKey");
        assertThat(generated).isEqualTo("axon-metadata-myKey");
        assertThat(HeaderUtils.extractKey(generated)).isEqualTo("myKey");
    }

    @Test
    void extractKeyRejectsInvalidPrefix() {
        assertThatThrownBy(() -> HeaderUtils.extractKey("invalid-prefix-key"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void extractAxonMetadata() {
        RecordHeaders headers = new RecordHeaders();
        headers.add("axon-metadata-foo", "bar".getBytes(StandardCharsets.UTF_8));
        headers.add("axon-metadata-baz", "qux".getBytes(StandardCharsets.UTF_8));
        headers.add("non-axon-key", "ignored".getBytes(StandardCharsets.UTF_8));

        Map<String, String> metadata = HeaderUtils.extractAxonMetadata(headers);
        assertThat(metadata).hasSize(2)
                            .containsEntry("foo", "bar")
                            .containsEntry("baz", "qux");
    }

    @Test
    void byteMapperConvertsStringValues() {
        var mapper = HeaderUtils.byteMapper();
        var header = mapper.apply("key", "value");
        assertThat(header.key()).isEqualTo("key");
        assertThat(new String(header.value(), StandardCharsets.UTF_8)).isEqualTo("value");
    }

    @Test
    void byteMapperPassesThroughByteArrays() {
        var mapper = HeaderUtils.byteMapper();
        byte[] raw = {1, 2, 3};
        var header = mapper.apply("key", raw);
        assertThat(header.value()).isSameAs(raw);
    }

    @Test
    void byteMapperHandlesNull() {
        var mapper = HeaderUtils.byteMapper();
        var header = mapper.apply("key", null);
        assertThat(header.value()).isNull();
    }

    @Test
    void toBytesConvertsShort() {
        byte[] bytes = HeaderUtils.toBytes((short) 5);
        short result = ByteBuffer.wrap(bytes).getShort();
        assertThat(result).isEqualTo((short) 5);
    }

    @Test
    void toBytesConvertsInteger() {
        byte[] bytes = HeaderUtils.toBytes(42);
        int result = ByteBuffer.wrap(bytes).getInt();
        assertThat(result).isEqualTo(42);
    }

    @Test
    void toBytesConvertsLong() {
        byte[] bytes = HeaderUtils.toBytes(123456789L);
        long result = ByteBuffer.wrap(bytes).getLong();
        assertThat(result).isEqualTo(123456789L);
    }

    @Test
    void toBytesConvertsFloat() {
        byte[] bytes = HeaderUtils.toBytes(3.14f);
        float result = ByteBuffer.wrap(bytes).getFloat();
        assertThat(result).isEqualTo(3.14f);
    }

    @Test
    void toBytesConvertsDouble() {
        byte[] bytes = HeaderUtils.toBytes(2.718);
        double result = ByteBuffer.wrap(bytes).getDouble();
        assertThat(result).isEqualTo(2.718);
    }

    @Test
    void addHeaderHandlesInstant() {
        RecordHeaders headers = new RecordHeaders();
        java.time.Instant instant = java.time.Instant.ofEpochMilli(1700000000000L);
        HeaderUtils.addHeader(headers, "ts", instant);

        assertThat(HeaderUtils.valueAsLong(headers, "ts")).isEqualTo(1700000000000L);
    }
}
