package com.example.mbsagg.journal.kafka.aggregator;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.CommonContainerStoppingErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.ContainerProperties.AssignmentCommitOption;

import com.example.mbsagg.journal.config.MbsAggProperties;

@Configuration(proxyBeanMethods = false)
@Profile("aggregator")
class AggregatorConfig {
    static final String CONTAINER_FACTORY = "aggregatorContainerFactory";

    /**
     * The aggregator's container. It does <em>not</em> run container-managed transactions: a flush spans several polls,
     * must write Redis before Kafka, must hold a replaying partition's offset at C and must carry the business date as
     * offset metadata. So the container never commits ({@code MANUAL}, never acknowledged) and
     * {@link KafkaTransactionalSink} commits the offsets inside its {@code @Transactional} Kafka transaction.
     *
     * <ul>
     *   <li>one consumer thread per pod, which owns every {@code ShardWorker}; scale out with pods</li>
     *   <li>idle events at the flush interval drive the time-based flush when no records arrive</li>
     *   <li>any error stops the container and the pod restarts ({@code ExitOnListenerFailure})</li>
     * </ul>
     */
    @Bean(CONTAINER_FACTORY)
    ConcurrentKafkaListenerContainerFactory<Object, Object> aggregatorContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory, JournalAggregator aggregator, MbsAggProperties properties) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        factory.setConcurrency(1);
        factory.setCommonErrorHandler(new CommonContainerStoppingErrorHandler());
        ContainerProperties c = factory.getContainerProperties();
        c.setAckMode(AckMode.MANUAL);
        c.setAssignmentCommitOption(AssignmentCommitOption.NEVER);
        c.setConsumerRebalanceListener(aggregator.rebalanceListener());
        c.setPollTimeout(properties.aggregator().pollTimeout().toMillis());
        c.setIdleEventInterval(properties.aggregator().flushInterval().toMillis());
        return factory;
    }
}
