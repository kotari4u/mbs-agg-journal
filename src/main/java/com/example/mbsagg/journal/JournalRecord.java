package com.example.mbsagg.journal;

import java.util.Objects;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * One record of the journal topic: a feed message, re-published by the sequencer keyed by CUSIP, so every update
 * for a CUSIP lands in one journal partition in one order.
 *
 * @param version the message's offset in its own source partition (or a source sequence). Only grows per field;
 *                never the journal offset, which changes when the sequencer re-sends.
 */
public record JournalRecord(FeedSource source, String bizDate, String cusip, String containerKey, long version,
                            JsonNode value) {
    private static final JsonMapper MAPPER = JsonMapper.shared();

    public JournalRecord {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(bizDate, "bizDate");
        Objects.requireNonNull(cusip, "cusip");
        Objects.requireNonNull(value, "value");
        if (source == FeedSource.POSITION && containerKey == null) {
            throw new IllegalArgumentException("a position needs a container key");
        }
    }

    /** {@code productId|inventoryId|cusip}. */
    public static String containerKey(String productId, String inventoryId, String cusip) {
        return productId + "|" + inventoryId + "|" + cusip;
    }

    public static String cusipOf(String containerKey) {
        return containerKey.substring(containerKey.lastIndexOf('|') + 1);
    }

    public String toJson() {
        ObjectNode n = MAPPER.createObjectNode();
        n.put("src", source.name());
        n.put("bizDate", bizDate);
        n.put("cusip", cusip);
        if (containerKey != null) n.put("ck", containerKey);
        n.put("ver", version);
        n.set("value", value);
        return n.toString();
    }

    public static JournalRecord fromJson(String json) {
        try {
            JsonNode n = MAPPER.readTree(json);
            return new JournalRecord(FeedSource.valueOf(n.get("src").asString()), n.get("bizDate").asString(),
                    n.get("cusip").asString(), n.hasNonNull("ck") ? n.get("ck").asString() : null, n.get("ver").asLong(),
                    n.get("value"));
        } catch (Exception e) {
            throw new IllegalArgumentException("bad journal record: " + json, e);
        }
    }
}
