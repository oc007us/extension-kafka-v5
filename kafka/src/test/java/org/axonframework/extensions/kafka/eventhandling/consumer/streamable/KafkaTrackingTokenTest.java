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

import org.apache.kafka.common.TopicPartition;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaTrackingTokenTest {

    @Test
    void emptyTokenHasNoPositions() {
        KafkaTrackingToken token = KafkaTrackingToken.emptyToken();
        assertThat(token.getPositions()).isEmpty();
        assertThat(KafkaTrackingToken.isEmpty(token)).isTrue();
        assertThat(KafkaTrackingToken.isNotEmpty(token)).isFalse();
    }

    @Test
    void isEmptyReturnsTrueForNull() {
        assertThat(KafkaTrackingToken.isEmpty(null)).isTrue();
    }

    @Test
    void newInstanceCreatesTokenFromPositions() {
        Map<TopicPartition, Long> positions = new HashMap<>();
        positions.put(new TopicPartition("topic-a", 0), 10L);
        positions.put(new TopicPartition("topic-a", 1), 20L);

        KafkaTrackingToken token = KafkaTrackingToken.newInstance(positions);

        assertThat(token.getPositions()).hasSize(2);
        assertThat(token.getPositions().get(new TopicPartition("topic-a", 0))).isEqualTo(10L);
        assertThat(KafkaTrackingToken.isNotEmpty(token)).isTrue();
    }

    @Test
    void advancedToCreatesNewTokenWithUpdatedOffset() {
        KafkaTrackingToken token = KafkaTrackingToken.emptyToken();

        KafkaTrackingToken advanced = token.advancedTo("events", 0, 5L);

        assertThat(advanced.getPositions()).hasSize(1);
        assertThat(advanced.getPositions().get(new TopicPartition("events", 0))).isEqualTo(5L);
        assertThat(token.getPositions()).isEmpty();
    }

    @Test
    void advancedToReplacesExistingOffset() {
        KafkaTrackingToken token = KafkaTrackingToken.emptyToken()
                                                      .advancedTo("events", 0, 5L);

        KafkaTrackingToken advanced = token.advancedTo("events", 0, 10L);

        assertThat(advanced.getPositions().get(new TopicPartition("events", 0))).isEqualTo(10L);
    }

    @Test
    void advancedToRejectsNullTopic() {
        KafkaTrackingToken token = KafkaTrackingToken.emptyToken();
        assertThatThrownBy(() -> token.advancedTo(null, 0, 0L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void advancedToRejectsNegativePartition() {
        KafkaTrackingToken token = KafkaTrackingToken.emptyToken();
        assertThatThrownBy(() -> token.advancedTo("topic", -1, 0L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void advancedToRejectsNegativeOffset() {
        KafkaTrackingToken token = KafkaTrackingToken.emptyToken();
        assertThatThrownBy(() -> token.advancedTo("topic", 0, -1L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void fromReturnsEmptyTokenForNull() {
        KafkaTrackingToken result = KafkaTrackingToken.from(null);
        assertThat(result).isNotNull();
        assertThat(KafkaTrackingToken.isEmpty(result)).isTrue();
    }

    @Test
    void fromReturnsSameInstanceForKafkaToken() {
        KafkaTrackingToken original = KafkaTrackingToken.emptyToken().advancedTo("t", 0, 1L);
        KafkaTrackingToken result = KafkaTrackingToken.from(original);
        assertThat(result).isSameAs(original);
    }

    @Test
    void fromRejectsIncompatibleTokenType() {
        TrackingToken incompatible = new TrackingToken() {
            @Override
            public TrackingToken lowerBound(TrackingToken other) { return this; }
            @Override
            public TrackingToken upperBound(TrackingToken other) { return this; }
            @Override
            public boolean covers(TrackingToken other) { return false; }
        };
        assertThatThrownBy(() -> KafkaTrackingToken.from(incompatible))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void lowerBoundTakesMinimumOffsets() {
        KafkaTrackingToken tokenA = KafkaTrackingToken.emptyToken()
                                                       .advancedTo("t", 0, 10L)
                                                       .advancedTo("t", 1, 5L);
        KafkaTrackingToken tokenB = KafkaTrackingToken.emptyToken()
                                                       .advancedTo("t", 0, 3L)
                                                       .advancedTo("t", 1, 8L);

        KafkaTrackingToken lower = (KafkaTrackingToken) tokenA.lowerBound(tokenB);

        assertThat(lower.getPositions().get(new TopicPartition("t", 0))).isEqualTo(3L);
        assertThat(lower.getPositions().get(new TopicPartition("t", 1))).isEqualTo(5L);
    }

    @Test
    void upperBoundTakesMaximumOffsets() {
        KafkaTrackingToken tokenA = KafkaTrackingToken.emptyToken()
                                                       .advancedTo("t", 0, 10L)
                                                       .advancedTo("t", 1, 5L);
        KafkaTrackingToken tokenB = KafkaTrackingToken.emptyToken()
                                                       .advancedTo("t", 0, 3L)
                                                       .advancedTo("t", 1, 8L);

        KafkaTrackingToken upper = (KafkaTrackingToken) tokenA.upperBound(tokenB);

        assertThat(upper.getPositions().get(new TopicPartition("t", 0))).isEqualTo(10L);
        assertThat(upper.getPositions().get(new TopicPartition("t", 1))).isEqualTo(8L);
    }

    @Test
    void boundsIncludePartitionsFromBothTokens() {
        KafkaTrackingToken tokenA = KafkaTrackingToken.emptyToken().advancedTo("t", 0, 5L);
        KafkaTrackingToken tokenB = KafkaTrackingToken.emptyToken().advancedTo("t", 1, 3L);

        KafkaTrackingToken lower = (KafkaTrackingToken) tokenA.lowerBound(tokenB);

        assertThat(lower.getPositions()).containsKeys(
                new TopicPartition("t", 0),
                new TopicPartition("t", 1)
        );
    }

    @Test
    void coversReturnsTrueWhenAllOffsetsAreCovered() {
        KafkaTrackingToken covering = KafkaTrackingToken.emptyToken()
                                                         .advancedTo("t", 0, 10L)
                                                         .advancedTo("t", 1, 20L);
        KafkaTrackingToken covered = KafkaTrackingToken.emptyToken()
                                                        .advancedTo("t", 0, 5L)
                                                        .advancedTo("t", 1, 15L);

        assertThat(covering.covers(covered)).isTrue();
    }

    @Test
    void coversReturnsFalseWhenOffsetIsAhead() {
        KafkaTrackingToken token = KafkaTrackingToken.emptyToken().advancedTo("t", 0, 5L);
        KafkaTrackingToken ahead = KafkaTrackingToken.emptyToken().advancedTo("t", 0, 10L);

        assertThat(token.covers(ahead)).isFalse();
    }

    @Test
    void coversReturnsFalseForUnknownPartition() {
        KafkaTrackingToken token = KafkaTrackingToken.emptyToken().advancedTo("t", 0, 5L);
        KafkaTrackingToken other = KafkaTrackingToken.emptyToken().advancedTo("t", 1, 3L);

        assertThat(token.covers(other)).isFalse();
    }

    @Test
    void equalsAndHashCode() {
        KafkaTrackingToken token1 = KafkaTrackingToken.emptyToken().advancedTo("t", 0, 5L);
        KafkaTrackingToken token2 = KafkaTrackingToken.emptyToken().advancedTo("t", 0, 5L);
        KafkaTrackingToken token3 = KafkaTrackingToken.emptyToken().advancedTo("t", 0, 6L);

        assertThat(token1).isEqualTo(token2);
        assertThat(token1).hasSameHashCodeAs(token2);
        assertThat(token1).isNotEqualTo(token3);
    }

    @Test
    void positionsMapIsImmutable() {
        Map<TopicPartition, Long> positions = new HashMap<>();
        positions.put(new TopicPartition("t", 0), 5L);
        KafkaTrackingToken token = KafkaTrackingToken.newInstance(positions);

        assertThatThrownBy(() -> token.getPositions().put(new TopicPartition("t", 1), 10L))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
