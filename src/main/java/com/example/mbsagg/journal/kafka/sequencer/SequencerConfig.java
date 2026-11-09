package com.example.mbsagg.journal.kafka.sequencer;

import org.springframework.boot.kafka.autoconfigure.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonContainerStoppingErrorHandler;
import org.springframework.util.Assert;

import com.example.mbsagg.journal.config.MbsAggProperties;

@Configuration(proxyBeanMethods = false)
@Profile("sequencer")
class SequencerConfig {
    static final String CONTAINER_FACTORY = "sequencerContainerFactory";

    /**
     * Boot's configurer applies {@code spring.kafka.listener.*} and the auto-configured
     * {@code KafkaTransactionManager}, so every poll runs in one Kafka transaction: the journal records, any dead
     * letters and the feed offsets commit together. Any failure stops the container (see {@code ExitOnListenerFailure}).
     */
    @Bean(CONTAINER_FACTORY)
    ConcurrentKafkaListenerContainerFactory<Object, Object> sequencerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer, ConsumerFactory<Object, Object> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);
        Assert.state(factory.getContainerProperties().getKafkaAwareTransactionManager() != null,
                "the sequencer needs spring.kafka.producer.transaction-id-prefix (Kafka transactions)");
        factory.setBatchListener(true);
        factory.setCommonErrorHandler(new CommonContainerStoppingErrorHandler());
        return factory;
    }

    @Bean
    DeadLetterHandler deadLetterHandler(KafkaTemplate<String, String> kafka, MbsAggProperties properties) {
        String topic = properties.sequencer().deadLetterTopic();
        return topic == null || topic.isBlank() ? DeadLetterHandler.logging() : DeadLetterHandler.toTopic(kafka, topic);
    }
}
