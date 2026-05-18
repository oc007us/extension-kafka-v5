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

package org.axonframework.extensions.kafka.eventhandling.producer;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.messaging.core.MessageType;

import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
class KafkaEventPublisherTest {

    @Test
    void handleDelegatesToKafkaPublisher() {
        KafkaPublisher<String, byte[]> publisher = mock(KafkaPublisher.class);

        KafkaEventPublisher<String, byte[]> eventPublisher = KafkaEventPublisher.<String, byte[]>builder()
                .kafkaPublisher(publisher)
                .build();

        EventMessage event = new GenericEventMessage(
                "id", new MessageType("Test"), "payload", Metadata.emptyInstance(), Instant.now()
        );

        eventPublisher.handle(event, null);

        verify(publisher).send(event, null);
    }

    @Test
    void defaultProcessingGroup() {
        KafkaPublisher<String, byte[]> publisher = mock(KafkaPublisher.class);

        KafkaEventPublisher<String, byte[]> eventPublisher = KafkaEventPublisher.<String, byte[]>builder()
                .kafkaPublisher(publisher)
                .build();

        assertThat(eventPublisher.getProcessingGroup())
                .isEqualTo(KafkaEventPublisher.DEFAULT_PROCESSING_GROUP);
    }

    @Test
    void customProcessingGroup() {
        KafkaPublisher<String, byte[]> publisher = mock(KafkaPublisher.class);

        KafkaEventPublisher<String, byte[]> eventPublisher = KafkaEventPublisher.<String, byte[]>builder()
                .kafkaPublisher(publisher)
                .processingGroup("custom-group")
                .build();

        assertThat(eventPublisher.getProcessingGroup()).isEqualTo("custom-group");
    }

    @Test
    void builderRequiresKafkaPublisher() {
        assertThatThrownBy(() -> KafkaEventPublisher.<String, byte[]>builder().build())
                .isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void builderRejectsNullProcessingGroup() {
        KafkaPublisher<String, byte[]> publisher = mock(KafkaPublisher.class);
        assertThatThrownBy(() -> KafkaEventPublisher.<String, byte[]>builder()
                .kafkaPublisher(publisher)
                .processingGroup(null)
                .build())
                .isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void builderRejectsEmptyProcessingGroup() {
        KafkaPublisher<String, byte[]> publisher = mock(KafkaPublisher.class);
        assertThatThrownBy(() -> KafkaEventPublisher.<String, byte[]>builder()
                .kafkaPublisher(publisher)
                .processingGroup("")
                .build())
                .isInstanceOf(AxonConfigurationException.class);
    }
}
