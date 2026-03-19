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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.Comparator;

import static org.axonframework.common.Assert.notNull;

/**
 * Wrapper around an {@link EventMessage} containing additional required information to correctly publish an
 * {@link EventMessage} over a Kafka topic.
 * <p>
 * In AF5 there is no separate {@code TrackedEventMessage} type; the tracking token is carried externally.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 * @since 4.0
 */
public class KafkaEventMessage implements KafkaRecordMetaData<EventMessage>, Comparable<KafkaEventMessage> {

    private static final Comparator<KafkaEventMessage> MESSAGE_COMPARATOR =
            Comparator.comparing(KafkaEventMessage::timestamp)
                      .thenComparing(KafkaEventMessage::partition)
                      .thenComparing(KafkaEventMessage::offset);

    private final EventMessage eventMessage;
    private final KafkaTrackingToken trackingToken;
    private final int partition;
    private final long offset;
    private final long timestamp;

    /**
     * Construct a Kafka {@link EventMessage} wrapper, encapsulating the given {@code eventMessage} and providing
     * additional information, like the Kafka {@code partition}, {@code offset} and {@code timestamp}.
     *
     * @param eventMessage  the {@link EventMessage} to wrap
     * @param trackingToken the {@link KafkaTrackingToken} defining the position of this message
     * @param partition     the partition the wrapped record originates from
     * @param offset        the position of the wrapped record in the corresponding Kafka {@code partition}
     * @param timestamp     the timestamp of the wrapped record
     */
    public KafkaEventMessage(EventMessage eventMessage, KafkaTrackingToken trackingToken,
                             int partition, long offset, long timestamp) {
        notNull(eventMessage, () -> "Event Message may not be null");
        notNull(trackingToken, () -> "Tracking Token may not be null");
        this.eventMessage = eventMessage;
        this.trackingToken = trackingToken;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
    }

    /**
     * Construct a {@link KafkaEventMessage} based on the deserialized body, the {@code eventMessage}, of a
     * {@link ConsumerRecord} retrieved from a Kafka topic.
     *
     * @param eventMessage   the {@link EventMessage} to wrap
     * @param consumerRecord the {@link ConsumerRecord} which the given {@code eventMessage} was the body of
     * @param trackingToken  the {@link KafkaTrackingToken} defining the position of this message
     * @return the {@link KafkaEventMessage} constructed from the given parameters
     */
    public static KafkaEventMessage from(EventMessage eventMessage,
                                         ConsumerRecord<?, ?> consumerRecord,
                                         KafkaTrackingToken trackingToken) {
        return new KafkaEventMessage(
                eventMessage, trackingToken,
                consumerRecord.partition(), consumerRecord.offset(), consumerRecord.timestamp()
        );
    }

    @Override
    public int partition() {
        return partition;
    }

    @Override
    public long offset() {
        return offset;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public EventMessage value() {
        return eventMessage;
    }

    /**
     * Return the {@link KafkaTrackingToken} associated with this message.
     *
     * @return the tracking token for this message
     */
    public KafkaTrackingToken trackingToken() {
        return trackingToken;
    }

    /**
     * Compares {@link ConsumerRecord} based on timestamp. If two records are published at the same time and belong
     * to:
     * <ul>
     * <li>a). The same partition; then return the one with smaller offset.</li>
     * <li>b). Different partitions; then return any.</li>
     * </ul>
     */
    @Override
    public int compareTo(KafkaEventMessage other) {
        return MESSAGE_COMPARATOR.compare(this, other);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof KafkaEventMessage otherMessage) {
            return this.timestamp == otherMessage.timestamp &&
                    this.partition == otherMessage.partition &&
                    this.offset == otherMessage.offset;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = (int) (31 * hash + this.timestamp);
        hash = 31 * hash + partition;
        hash = (int) (31 * hash + offset);
        return hash;
    }

    @Override
    public String toString() {
        return "KafkaEventMessage{" +
                "eventMessage=" + eventMessage +
                ", partition=" + partition +
                ", offset=" + offset +
                ", timestamp=" + timestamp +
                '}';
    }
}
