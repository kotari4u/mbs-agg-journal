package com.example.mbsagg.journal.kafka.aggregator;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.stereotype.Component;

/**
 * The aggregator's listener endpoints: journal batches (static membership, {@code read_committed}) and the
 * container's idle events, both on the consumer thread, handed to {@link JournalAggregator}.
 */
@Component
@Profile("aggregator")
class JournalListener {
    static final String LISTENER_ID = "mbs-aggregator";

    private final JournalAggregator aggregator;

    JournalListener(JournalAggregator aggregator) {
        this.aggregator = aggregator;
    }

    @KafkaListener(id = LISTENER_ID, groupId = "${mbsagg.aggregator.group-id}", topics = "${mbsagg.topics.journal}",
            containerFactory = AggregatorConfig.CONTAINER_FACTORY,
            properties = "group.instance.id=${mbsagg.aggregator.instance-id}")
    void onJournal(ConsumerRecords<String, String> records, Consumer<?, ?> consumer) {
        aggregator.onRecords(records, consumer);
    }

    @EventListener(condition = "event.listenerId.startsWith('" + LISTENER_ID + "')")
    void onIdle(ListenerContainerIdleEvent event) {
        aggregator.onIdle(event.getConsumer());
    }
}
