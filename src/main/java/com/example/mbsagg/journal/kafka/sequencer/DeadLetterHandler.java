package com.example.mbsagg.journal.kafka.sequencer;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaOperations;

/** Receives feed records the sequencer can't parse. */
@FunctionalInterface
public interface DeadLetterHandler {
    String REASON_HEADER = "mbsagg-dlt-reason";

    void deadLetter(ConsumerRecord<String, String> record, String reason);

    /** Logs and drops. */
    static DeadLetterHandler logging() {
        Logger log = LoggerFactory.getLogger(DeadLetterHandler.class);
        return (r, reason) -> log.error("dead letter {}-{}@{}: {}", r.topic(), r.partition(), r.offset(), reason);
    }

    /**
     * Publishes the original record, with its headers and a reason header, to {@code topic}. Inside the sequencer's
     * listener this send joins the container's Kafka transaction, so it commits together with the feed offsets.
     */
    static DeadLetterHandler toTopic(KafkaOperations<String, String> kafka, String topic) {
        return (r, reason) -> {
            ProducerRecord<String, String> out = new ProducerRecord<>(topic, null, r.key(), r.value(), r.headers());
            out.headers().add(REASON_HEADER, String.valueOf(reason).getBytes(StandardCharsets.UTF_8));
            kafka.send(out);
        };
    }
}
