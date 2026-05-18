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

import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SortedKafkaMessageBufferTest {

    @Test
    void defaultCapacityIs1000() {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        assertThat(buffer.remainingCapacity()).isEqualTo(1000);
    }

    @Test
    void customCapacity() {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>(5);
        assertThat(buffer.remainingCapacity()).isEqualTo(5);
    }

    @Test
    void rejectsZeroCapacity() {
        assertThatThrownBy(() -> new SortedKafkaMessageBuffer<>(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void putAndPollInSortedOrder() throws InterruptedException {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();

        buffer.put(createMessage(0, 2L, 2000L));
        buffer.put(createMessage(0, 0L, 1000L));
        buffer.put(createMessage(0, 1L, 1500L));

        KafkaEventMessage first = buffer.poll(100, TimeUnit.MILLISECONDS);
        KafkaEventMessage second = buffer.poll(100, TimeUnit.MILLISECONDS);
        KafkaEventMessage third = buffer.poll(100, TimeUnit.MILLISECONDS);

        assertThat(first.offset()).isEqualTo(0L);
        assertThat(second.offset()).isEqualTo(1L);
        assertThat(third.offset()).isEqualTo(2L);
    }

    @Test
    void putAllAddsMultipleElements() throws InterruptedException {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();

        buffer.putAll(List.of(
                createMessage(0, 1L, 2000L),
                createMessage(0, 0L, 1000L)
        ));

        assertThat(buffer.size()).isEqualTo(2);
        assertThat(buffer.poll(100, TimeUnit.MILLISECONDS).offset()).isEqualTo(0L);
    }

    @Test
    void peekDoesNotRemoveElement() throws InterruptedException {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        buffer.put(createMessage(0, 0L, 1000L));

        assertThat(buffer.peek()).isNotNull();
        assertThat(buffer.peek()).isNotNull();
        assertThat(buffer.size()).isEqualTo(1);
    }

    @Test
    void peekReturnsNullWhenEmpty() {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        assertThat(buffer.peek()).isNull();
    }

    @Test
    void pollReturnsNullOnTimeout() throws InterruptedException {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        assertThat(buffer.poll(10, TimeUnit.MILLISECONDS)).isNull();
    }

    @Test
    void isEmptyAndSize() throws InterruptedException {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        assertThat(buffer.isEmpty()).isTrue();
        assertThat(buffer.size()).isZero();

        buffer.put(createMessage(0, 0L, 1000L));
        assertThat(buffer.isEmpty()).isFalse();
        assertThat(buffer.size()).isEqualTo(1);
    }

    @Test
    void clearRemovesAllElements() throws InterruptedException {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        buffer.put(createMessage(0, 0L, 1000L));
        buffer.put(createMessage(0, 1L, 2000L));

        buffer.clear();
        assertThat(buffer.peek()).isNull();
    }

    @Test
    void remainingCapacityDecreasesOnPut() throws InterruptedException {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>(10);
        buffer.put(createMessage(0, 0L, 1000L));

        assertThat(buffer.remainingCapacity()).isEqualTo(9);
    }

    @Test
    void setExceptionCausesThrowOnPeek() throws InterruptedException {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        RuntimeException expected = new RuntimeException("test");

        buffer.setException(expected);

        assertThatThrownBy(buffer::peek)
                .isSameAs(expected);
    }

    @Test
    void onDataAvailableCalledOnPut() throws InterruptedException {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        AtomicBoolean called = new AtomicBoolean(false);
        buffer.setOnDataAvailable(() -> called.set(true));

        buffer.put(createMessage(0, 0L, 1000L));

        assertThat(called).isTrue();
    }

    @Test
    void duplicateElementsAreNotAdded() throws InterruptedException {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        KafkaEventMessage msg = createMessage(0, 0L, 1000L);

        buffer.put(msg);
        buffer.put(msg);

        assertThat(buffer.size()).isEqualTo(1);
    }

    private KafkaEventMessage createMessage(int partition, long offset, long timestamp) {
        EventMessage event = new GenericEventMessage(
                "id-" + offset,
                new MessageType("TestEvent"),
                "payload",
                Metadata.emptyInstance(),
                Instant.ofEpochMilli(timestamp)
        );
        KafkaTrackingToken token = KafkaTrackingToken.emptyToken().advancedTo("topic", partition, offset);
        return new KafkaEventMessage(event, token, partition, offset, timestamp);
    }
}
