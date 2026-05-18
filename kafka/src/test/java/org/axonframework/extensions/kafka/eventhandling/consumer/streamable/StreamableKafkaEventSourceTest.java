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

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.Fetcher;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

@SuppressWarnings("unchecked")
class StreamableKafkaEventSourceTest {

    @Test
    void builderRequiresConsumerFactory() {
        assertThatThrownBy(() -> StreamableKafkaEventSource.<String, byte[]>builder()
                .fetcher(mock(Fetcher.class))
                .messageConverter(mock(KafkaMessageConverter.class))
                .build())
                .isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void builderRequiresFetcher() {
        assertThatThrownBy(() -> StreamableKafkaEventSource.<String, byte[]>builder()
                .consumerFactory(mock(ConsumerFactory.class))
                .messageConverter(mock(KafkaMessageConverter.class))
                .build())
                .isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void builderRequiresMessageConverter() {
        assertThatThrownBy(() -> StreamableKafkaEventSource.<String, byte[]>builder()
                .consumerFactory(mock(ConsumerFactory.class))
                .fetcher(mock(Fetcher.class))
                .build())
                .isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void builderSucceedsWithAllRequiredFields() {
        assertThatCode(() -> StreamableKafkaEventSource.<String, byte[]>builder()
                .consumerFactory(mock(ConsumerFactory.class))
                .fetcher(mock(Fetcher.class))
                .messageConverter(mock(KafkaMessageConverter.class))
                .build())
                .doesNotThrowAnyException();
    }

    @Test
    void firstTokenReturnsEmptyKafkaTrackingToken() throws Exception {
        StreamableKafkaEventSource<String, byte[]> source = StreamableKafkaEventSource.<String, byte[]>builder()
                .consumerFactory(mock(ConsumerFactory.class))
                .fetcher(mock(Fetcher.class))
                .messageConverter(mock(KafkaMessageConverter.class))
                .build();

        TrackingToken token = source.firstToken(null).get();
        assertThat(token).isInstanceOf(KafkaTrackingToken.class);
        assertThat(KafkaTrackingToken.isEmpty((KafkaTrackingToken) token)).isTrue();
    }
}
