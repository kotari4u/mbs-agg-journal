package com.example.mbsagg.journal.kafka.sequencer;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.example.mbsagg.journal.FeedSource;
import com.example.mbsagg.journal.JournalRecord;
import com.example.mbsagg.journal.config.MbsAggProperties;
import com.example.mbsagg.journal.kafka.JsonJournalParser;

/**
 * Re-publishes the four feeds into the journal topic, keyed by CUSIP, so every update for a CUSIP lands in one
 * journal partition. Each record carries its source offset as its version.
 *
 * <p>Each poll is one Kafka transaction started by the listener container: the journal records and the feed offsets
 * commit together, and the aggregator reads the journal {@code read_committed}. A rolled-back or retried poll can
 * still re-send records under a new journal offset, and the aggregator's version check makes those harmless.
 */
@Component
@Profile("sequencer")
class JournalSequencer {
    static final String LISTENER_ID = "mbs-sequencer";

    private final KafkaTemplate<String, String> kafka;
    private final JsonJournalParser parser;
    private final DeadLetterHandler deadLetters;
    private final String journalTopic;
    private final Map<String, FeedSource> feedTopics;

    JournalSequencer(KafkaTemplate<String, String> kafka, JsonJournalParser parser, DeadLetterHandler deadLetters,
                     MbsAggProperties properties) {
        this.kafka = kafka;
        this.parser = parser;
        this.deadLetters = deadLetters;
        MbsAggProperties.Topics t = properties.topics();
        this.journalTopic = t.journal();
        this.feedTopics = Map.of(t.position(), FeedSource.POSITION, t.price(), FeedSource.PRICE,
                t.risk(), FeedSource.RISK, t.assumption(), FeedSource.ASSUMPTION);
    }

    @KafkaListener(id = LISTENER_ID, groupId = "${mbsagg.sequencer.group-id}",
            containerFactory = SequencerConfig.CONTAINER_FACTORY,
            topics = {"${mbsagg.topics.position}", "${mbsagg.topics.price}", "${mbsagg.topics.risk}",
                    "${mbsagg.topics.assumption}"})
    void onFeeds(List<ConsumerRecord<String, String>> records) {
        for (ConsumerRecord<String, String> r : records) {
            JournalRecord j;
            try {
                j = parser.parse(feedTopics.get(r.topic()), r);
            } catch (IllegalArgumentException e) {
                deadLetters.deadLetter(r, e.getMessage());
                continue;
            }
            kafka.send(journalTopic, j.cusip(), j.toJson());   // joins the container's transaction
        }
    }
}
