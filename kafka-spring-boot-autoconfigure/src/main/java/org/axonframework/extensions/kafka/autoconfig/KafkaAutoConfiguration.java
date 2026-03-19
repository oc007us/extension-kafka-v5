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

package org.axonframework.extensions.kafka.autoconfig;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.axonframework.conversion.Converter;
import org.axonframework.extensions.kafka.KafkaProperties;
import org.axonframework.extensions.kafka.eventhandling.DefaultKafkaMessageConverter;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.DefaultConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.producer.ConfirmationMode;
import org.axonframework.extensions.kafka.eventhandling.producer.DefaultProducerFactory;
import org.axonframework.extensions.kafka.eventhandling.producer.KafkaEventPublisher;
import org.axonframework.extensions.kafka.eventhandling.producer.KafkaPublisher;
import org.axonframework.extensions.kafka.eventhandling.producer.ProducerFactory;
import org.axonframework.extensions.kafka.eventhandling.producer.TopicResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.lang.invoke.MethodHandles;
import java.util.Optional;

/**
 * Auto configuration for the Axon Kafka Extension as an Event Message distribution solution.
 * <p>
 * Adapted for Axon Framework 5 where {@link Converter} replaces Serializer and event messages are non-generic.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 * @since 4.0
 */
@AutoConfiguration
@ConditionalOnClass(KafkaProducer.class)
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaAutoConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Bean
    @ConditionalOnMissingBean
    public KafkaMessageConverter<String, byte[]> kafkaMessageConverter(Converter converter) {
        return DefaultKafkaMessageConverter.builder()
                                           .converter(converter)
                                           .build();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = "axon.kafka.publisher.enabled", havingValue = "true", matchIfMissing = true)
    public ProducerFactory<String, byte[]> kafkaProducerFactory(KafkaProperties properties) {
        ConfirmationMode confirmationMode = properties.getPublisher().getConfirmationMode();
        String transactionIdPrefix = properties.getProducer().getTransactionIdPrefix();

        DefaultProducerFactory.Builder<String, byte[]> builder =
                DefaultProducerFactory.<String, byte[]>builder()
                                      .configuration(properties.buildProducerProperties())
                                      .confirmationMode(confirmationMode);

        if (transactionIdPrefix != null && !transactionIdPrefix.isEmpty()) {
            builder.transactionalIdPrefix(transactionIdPrefix)
                   .confirmationMode(ConfirmationMode.TRANSACTIONAL);
            if (!confirmationMode.isTransactional()) {
                logger.warn(
                        "The confirmation mode is set to [{}], whilst a transactional id prefix is present. "
                                + "The transactional id prefix overwrites the confirmation mode choice to TRANSACTIONAL",
                        confirmationMode
                );
            }
        }

        return builder.build();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = "axon.kafka.publisher.enabled", havingValue = "true", matchIfMissing = true)
    public TopicResolver topicResolver(KafkaProperties properties) {
        return m -> Optional.of(properties.getDefaultTopic());
    }

    @Bean(destroyMethod = "shutDown")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = "axon.kafka.publisher.enabled", havingValue = "true", matchIfMissing = true)
    public KafkaPublisher<String, byte[]> kafkaPublisher(
            ProducerFactory<String, byte[]> producerFactory,
            KafkaMessageConverter<String, byte[]> converter,
            TopicResolver topicResolver,
            KafkaProperties properties) {
        return KafkaPublisher.<String, byte[]>builder()
                             .producerFactory(producerFactory)
                             .messageConverter(converter)
                             .topicResolver(topicResolver)
                             .publisherAckTimeout(properties.getPublisher().getAckTimeout())
                             .build();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = "axon.kafka.publisher.enabled", havingValue = "true", matchIfMissing = true)
    public KafkaEventPublisher<String, byte[]> kafkaEventPublisher(
            KafkaPublisher<String, byte[]> publisher,
            KafkaProperties properties) {
        return KafkaEventPublisher.<String, byte[]>builder()
                                  .kafkaPublisher(publisher)
                                  .processingGroup(properties.getPublisher().getProcessingGroup())
                                  .build();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = "axon.kafka.fetcher.enabled", havingValue = "true", matchIfMissing = true)
    public ConsumerFactory<String, byte[]> kafkaConsumerFactory(KafkaProperties properties) {
        return new DefaultConsumerFactory<>(properties.buildConsumerProperties());
    }
}
