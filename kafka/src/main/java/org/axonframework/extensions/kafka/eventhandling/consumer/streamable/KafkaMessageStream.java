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

import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.SimpleEntry;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static org.axonframework.common.BuilderUtils.assertNonNull;

/**
 * A {@link MessageStream} implementation that bridges a {@link SortedKafkaMessageBuffer} to the AF5 streaming API.
 * <p>
 * Messages are fetched in bulk by a background task and stored in the in-memory buffer. This stream polls from the
 * buffer in a non-blocking manner as required by the {@link MessageStream} contract.
 * <p>
 * Consumer position is tracked via {@link KafkaTrackingToken}, which is added to each entry's {@link Context}.
 *
 * @author Allard Buijze
 * @author Nakul Mishra
 * @since 5.0
 */
public class KafkaMessageStream implements MessageStream<EventMessage> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageStream.class);

    private final Buffer<KafkaEventMessage> buffer;
    private final Runnable closeHandler;
    private volatile Runnable callback;
    private volatile boolean closed;
    private volatile Throwable error;

    /**
     * Create a {@link MessageStream} dedicated to {@link KafkaEventMessage}s. Uses the provided {@code buffer} to
     * retrieve event messages from.
     *
     * @param buffer       the {@link KafkaEventMessage} {@link Buffer} containing the fetched messages
     * @param closeHandler the {@link Runnable} to invoke when this stream is closed, typically cancelling the fetcher
     */
    public KafkaMessageStream(Buffer<KafkaEventMessage> buffer, Runnable closeHandler) {
        assertNonNull(buffer, "Buffer may not be null");
        this.buffer = buffer;
        this.closeHandler = closeHandler != null ? closeHandler : () -> { };
    }

    @Override
    public Optional<Entry<EventMessage>> next() {
        if (closed || error != null) {
            return Optional.empty();
        }
        KafkaEventMessage msg = buffer.peek();
        if (msg != null) {
            // Remove the peeked element by polling with zero timeout
            try {
                msg = buffer.poll(0, java.util.concurrent.TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.warn("Consumer thread was interrupted while polling buffer.", e);
                Thread.currentThread().interrupt();
                return Optional.empty();
            }
            if (msg != null) {
                return Optional.of(toEntry(msg));
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<Entry<EventMessage>> peek() {
        if (closed || error != null) {
            return Optional.empty();
        }
        KafkaEventMessage msg = buffer.peek();
        if (msg != null) {
            return Optional.of(toEntry(msg));
        }
        return Optional.empty();
    }

    private Entry<EventMessage> toEntry(KafkaEventMessage msg) {
        Context ctx = TrackingToken.addToContext(Context.empty(), msg.trackingToken());
        return new SimpleEntry<>(msg.value(), ctx);
    }

    @Override
    public void setCallback(Runnable callback) {
        this.callback = callback;
        // Also set on buffer so it notifies when data arrives
        if (buffer instanceof SortedKafkaMessageBuffer<?> smb) {
            smb.setOnDataAvailable(callback);
        }
        // If data already available, notify immediately
        if (hasNextAvailable()) {
            callback.run();
        }
    }

    @Override
    public boolean hasNextAvailable() {
        return !closed && error == null && buffer.peek() != null;
    }

    @Override
    public boolean isCompleted() {
        return closed && buffer.peek() == null;
    }

    @Override
    public Optional<Throwable> error() {
        return Optional.ofNullable(error);
    }

    @Override
    public void close() {
        closed = true;
        closeHandler.run();
    }

    /**
     * Report an error on this stream. This will mark the stream as closed and notify any registered callback so that
     * the consumer can discover the error.
     *
     * @param t the error to report
     */
    public void reportError(Throwable t) {
        this.error = t;
        closed = true;
        Runnable cb = callback;
        if (cb != null) {
            cb.run();
        }
    }
}
