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
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaEventMessageTest {

    @Test
    void constructorStoresAllFields() {
        EventMessage event = testEvent();
        KafkaTrackingToken token = KafkaTrackingToken.emptyToken().advancedTo("t", 0, 5L);

        KafkaEventMessage msg = new KafkaEventMessage(event, token, 0, 5L, 1000L);

        assertThat(msg.value()).isSameAs(event);
        assertThat(msg.trackingToken()).isSameAs(token);
        assertThat(msg.partition()).isEqualTo(0);
        assertThat(msg.offset()).isEqualTo(5L);
        assertThat(msg.timestamp()).isEqualTo(1000L);
    }

    @Test
    void constructorRejectsNullEventMessage() {
        KafkaTrackingToken token = KafkaTrackingToken.emptyToken();
        assertThatThrownBy(() -> new KafkaEventMessage(null, token, 0, 0L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void constructorRejectsNullToken() {
        assertThatThrownBy(() -> new KafkaEventMessage(testEvent(), null, 0, 0L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void fromConsumerRecordCreatesCorrectMessage() {
        EventMessage event = testEvent();
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>("topic", 2, 42L, "key", "value".getBytes());
        KafkaTrackingToken token = KafkaTrackingToken.emptyToken().advancedTo("topic", 2, 42L);

        KafkaEventMessage msg = KafkaEventMessage.from(event, record, token);

        assertThat(msg.partition()).isEqualTo(2);
        assertThat(msg.offset()).isEqualTo(42L);
    }

    @Test
    void compareToSortsByTimestampThenPartitionThenOffset() {
        KafkaTrackingToken token = KafkaTrackingToken.emptyToken();
        EventMessage event = testEvent();

        KafkaEventMessage early = new KafkaEventMessage(event, token, 0, 0L, 1000L);
        KafkaEventMessage late = new KafkaEventMessage(event, token, 0, 0L, 2000L);
        assertThat(early.compareTo(late)).isLessThan(0);

        KafkaEventMessage sameTimeLowPartition = new KafkaEventMessage(event, token, 0, 0L, 1000L);
        KafkaEventMessage sameTimeHighPartition = new KafkaEventMessage(event, token, 1, 0L, 1000L);
        assertThat(sameTimeLowPartition.compareTo(sameTimeHighPartition)).isLessThan(0);

        KafkaEventMessage samePartitionLowOffset = new KafkaEventMessage(event, token, 0, 1L, 1000L);
        KafkaEventMessage samePartitionHighOffset = new KafkaEventMessage(event, token, 0, 5L, 1000L);
        assertThat(samePartitionLowOffset.compareTo(samePartitionHighOffset)).isLessThan(0);
    }

    @Test
    void equalsBasedOnTimestampPartitionAndOffset() {
        EventMessage event1 = testEvent();
        EventMessage event2 = new GenericEventMessage(
                "other-id", new MessageType("Other"), "other", Metadata.emptyInstance(), Instant.now()
        );
        KafkaTrackingToken token = KafkaTrackingToken.emptyToken();

        KafkaEventMessage msg1 = new KafkaEventMessage(event1, token, 0, 5L, 1000L);
        KafkaEventMessage msg2 = new KafkaEventMessage(event2, token, 0, 5L, 1000L);

        assertThat(msg1).isEqualTo(msg2);
        assertThat(msg1.hashCode()).isEqualTo(msg2.hashCode());
    }

    @Test
    void notEqualWhenDifferentOffset() {
        KafkaTrackingToken token = KafkaTrackingToken.emptyToken();
        EventMessage event = testEvent();

        KafkaEventMessage msg1 = new KafkaEventMessage(event, token, 0, 5L, 1000L);
        KafkaEventMessage msg2 = new KafkaEventMessage(event, token, 0, 6L, 1000L);

        assertThat(msg1).isNotEqualTo(msg2);
    }

    private EventMessage testEvent() {
        return new GenericEventMessage(
                "test-id", new MessageType("TestEvent"), "payload", Metadata.emptyInstance(), Instant.now()
        );
    }
}
