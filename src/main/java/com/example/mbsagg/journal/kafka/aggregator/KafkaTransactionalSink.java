package com.example.mbsagg.journal.kafka.aggregator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.example.mbsagg.journal.ContainerUpdate;
import com.example.mbsagg.journal.OffsetCommit;
import com.example.mbsagg.journal.config.MbsAggProperties;

/**
 * Step 2 of a flush: one Kafka transaction, managed by Spring's {@code KafkaTransactionManager}, with the
 * containers on the output topic and the journal offsets sent through {@code sendOffsetsToTransaction(groupMetadata)},
 * so a consumer that lost its partitions in a rebalance can't commit (KIP-447). The business date rides along as offset
 * metadata. Any exception rolls the transaction back (abort).
 */
@Component
@Profile("aggregator")
public class KafkaTransactionalSink {
    /** The bean name Spring Boot gives the auto-configured transaction manager. */
    public static final String TRANSACTION_MANAGER = "kafkaTransactionManager";

    private final KafkaTemplate<String, String> kafka;
    private final String journalTopic;
    private final String outputTopic;

    public KafkaTransactionalSink(KafkaTemplate<String, String> kafka, MbsAggProperties properties) {
        this.kafka = kafka;
        this.journalTopic = properties.topics().journal();
        this.outputTopic = properties.topics().output();
    }

    @Transactional(transactionManager = TRANSACTION_MANAGER)
    public void commit(List<ContainerUpdate> output, List<OffsetCommit> offsets, ConsumerGroupMetadata group) {
        for (ContainerUpdate u : output) kafka.send(outputTopic, u.containerKey(), u.json());
        Map<TopicPartition, OffsetAndMetadata> o = new HashMap<>();
        for (OffsetCommit c : offsets) {
            o.put(new TopicPartition(journalTopic, c.partition()), new OffsetAndMetadata(c.nextOffset(), c.bizDate()));
        }
        kafka.sendOffsetsToTransaction(o, group);
    }
}
