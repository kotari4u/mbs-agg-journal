package com.example.mbsagg.journal.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

/** The first journal offset that can belong to a business date, for a replay from the start of the day. */
@FunctionalInterface
public interface StartOfDay {
    /** @param consumer the listener's consumer; call only on its thread */
    long offset(Consumer<?, ?> consumer, TopicPartition partition, String bizDate);
}
