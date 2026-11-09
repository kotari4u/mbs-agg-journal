package com.example.mbsagg.journal.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

import com.example.mbsagg.journal.FeedSource;
import com.example.mbsagg.journal.JournalRecord;
import com.example.mbsagg.journal.config.BusinessCalendar;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Turns a JSON feed record into a journal record. Expects {@code cusip}, positions also {@code productId} and
 * {@code inventoryId}, and optionally {@code bizDate}; without it the business date comes from the record
 * timestamp. The version is the record's offset in its source partition. Adapt the field names to your feeds.
 */
@Component
public class JsonJournalParser {
    private final JsonMapper mapper;
    private final BusinessCalendar calendar;

    public JsonJournalParser(JsonMapper mapper, BusinessCalendar calendar) {
        this.mapper = mapper;
        this.calendar = calendar;
    }

    /** @throws IllegalArgumentException if the record is not a valid feed message */
    public JournalRecord parse(FeedSource source, ConsumerRecord<String, String> record) {
        if (record.value() == null) throw new IllegalArgumentException("null value");
        JsonNode n;
        try {
            n = mapper.readTree(record.value());
        } catch (JacksonException e) {
            throw new IllegalArgumentException("not JSON: " + e.getOriginalMessage(), e);
        }
        if (!n.isObject()) throw new IllegalArgumentException("not a JSON object");
        String cusip = required(n, "cusip");
        String bizDate = n.hasNonNull("bizDate") ? n.get("bizDate").asString() : calendar.businessDate(record.timestamp());
        String ck = source == FeedSource.POSITION
                ? JournalRecord.containerKey(required(n, "productId"), required(n, "inventoryId"), cusip) : null;
        return new JournalRecord(source, bizDate, cusip, ck, record.offset(), n);
    }

    private static String required(JsonNode n, String field) {
        String v = n.path(field).asString("");
        if (v.isEmpty()) throw new IllegalArgumentException("missing " + field);
        return v;
    }
}
