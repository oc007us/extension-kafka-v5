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

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Base test class that provides a shared Kafka container via Testcontainers.
 * <p>
 * Subclasses can obtain the bootstrap servers address through {@link #bootstrapServers()}.
 *
 * @author Axon Framework Contributors
 * @since 5.0
 */
@Testcontainers
abstract class KafkaContainerIntegrationTest {

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.0"));

    /**
     * Returns the bootstrap servers address for the running Kafka container.
     *
     * @return the bootstrap servers connection string
     */
    protected String bootstrapServers() {
        return KAFKA.getBootstrapServers();
    }
}
