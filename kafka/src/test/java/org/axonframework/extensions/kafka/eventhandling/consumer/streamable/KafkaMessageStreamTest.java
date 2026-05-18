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
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaMessageStreamTest {

    @Test
    void nextReturnsEmptyWhenBufferIsEmpty() {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        KafkaMessageStream stream = new KafkaMessageStream(buffer, () -> {});

        Optional<MessageStream.Entry<EventMessage>> result = stream.next();
        assertThat(result).isEmpty();
    }

    @Test
    void nextReturnsEntryWhenBufferHasData() throws InterruptedException {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        KafkaMessageStream stream = new KafkaMessageStream(buffer, () -> {});

        KafkaEventMessage msg = createTestMessage(0, 0L, 1000L);
        buffer.put(msg);

        Optional<MessageStream.Entry<EventMessage>> result = stream.next();
        assertThat(result).isPresent();
        assertThat(result.get().message().identifier()).isEqualTo("test-id");
    }

    @Test
    void peekDoesNotConsumeElement() throws InterruptedException {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        KafkaMessageStream stream = new KafkaMessageStream(buffer, () -> {});

        buffer.put(createTestMessage(0, 0L, 1000L));

        Optional<MessageStream.Entry<EventMessage>> peek1 = stream.peek();
        Optional<MessageStream.Entry<EventMessage>> peek2 = stream.peek();

        assertThat(peek1).isPresent();
        assertThat(peek2).isPresent();
        assertThat(buffer.size()).isEqualTo(1);
    }

    @Test
    void hasNextAvailableReflectsBufferState() throws InterruptedException {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        KafkaMessageStream stream = new KafkaMessageStream(buffer, () -> {});

        assertThat(stream.hasNextAvailable()).isFalse();

        buffer.put(createTestMessage(0, 0L, 1000L));
        assertThat(stream.hasNextAvailable()).isTrue();
    }

    @Test
    void closeInvokesCloseHandler() {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        AtomicBoolean closed = new AtomicBoolean(false);
        KafkaMessageStream stream = new KafkaMessageStream(buffer, () -> closed.set(true));

        stream.close();

        assertThat(closed).isTrue();
    }

    @Test
    void closedStreamReturnsEmpty() throws InterruptedException {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        KafkaMessageStream stream = new KafkaMessageStream(buffer, () -> {});

        buffer.put(createTestMessage(0, 0L, 1000L));
        stream.close();

        assertThat(stream.next()).isEmpty();
        assertThat(stream.peek()).isEmpty();
        assertThat(stream.hasNextAvailable()).isFalse();
    }

    @Test
    void isCompletedWhenClosedAndBufferEmpty() {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        KafkaMessageStream stream = new KafkaMessageStream(buffer, () -> {});

        assertThat(stream.isCompleted()).isFalse();

        stream.close();
        assertThat(stream.isCompleted()).isTrue();
    }

    @Test
    void reportErrorMakesStreamReturnEmpty() throws InterruptedException {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        KafkaMessageStream stream = new KafkaMessageStream(buffer, () -> {});

        buffer.put(createTestMessage(0, 0L, 1000L));

        RuntimeException error = new RuntimeException("test error");
        stream.reportError(error);

        assertThat(stream.error()).contains(error);
        assertThat(stream.next()).isEmpty();
        assertThat(stream.hasNextAvailable()).isFalse();
    }

    @Test
    void reportErrorNotifiesCallback() {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        KafkaMessageStream stream = new KafkaMessageStream(buffer, () -> {});
        AtomicBoolean called = new AtomicBoolean(false);
        stream.setCallback(() -> called.set(true));

        stream.reportError(new RuntimeException("oops"));

        assertThat(called).isTrue();
    }

    @Test
    void setCallbackNotifiesImmediatelyWhenDataAvailable() throws InterruptedException {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        KafkaMessageStream stream = new KafkaMessageStream(buffer, () -> {});

        buffer.put(createTestMessage(0, 0L, 1000L));

        AtomicBoolean notified = new AtomicBoolean(false);
        stream.setCallback(() -> notified.set(true));

        assertThat(notified).isTrue();
    }

    @Test
    void entryContainsTrackingToken() throws InterruptedException {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        KafkaMessageStream stream = new KafkaMessageStream(buffer, () -> {});

        KafkaTrackingToken token = KafkaTrackingToken.emptyToken().advancedTo("topic", 0, 5L);
        EventMessage event = new GenericEventMessage(
                "test-id", new MessageType("TestEvent"), "payload", Metadata.emptyInstance(), Instant.now()
        );
        KafkaEventMessage msg = new KafkaEventMessage(event, token, 0, 5L, 1000L);
        buffer.put(msg);

        Optional<MessageStream.Entry<EventMessage>> entry = stream.next();
        assertThat(entry).isPresent();
    }

    private KafkaEventMessage createTestMessage(int partition, long offset, long timestamp) {
        EventMessage event = new GenericEventMessage(
                "test-id", new MessageType("TestEvent"), "payload", Metadata.emptyInstance(), Instant.now()
        );
        KafkaTrackingToken token = KafkaTrackingToken.emptyToken().advancedTo("topic", partition, offset);
        return new KafkaEventMessage(event, token, partition, offset, timestamp);
    }
}
