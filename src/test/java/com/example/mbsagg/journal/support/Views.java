package com.example.mbsagg.journal.support;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.example.mbsagg.journal.ContainerUpdate;
import com.example.mbsagg.journal.FeedSource;
import com.example.mbsagg.journal.JournalRecord;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.NullNode;
import tools.jackson.databind.node.ObjectNode;

/** The expected container views (highest version per field, computed independently) and comparison helpers. */
public final class Views {
    private static final JsonMapper MAPPER = JsonMapper.shared();
    private static final String[] FIELDS = {"price", "risk", "assumptions"};

    private Views() {
    }

    /** The oracle: for every field, the record with the highest version wins. */
    public static Map<String, String> expected(List<JournalRecord> journal) {
        Map<String, JournalRecord> best = new HashMap<>();
        for (JournalRecord r : journal) {
            String k = r.source() + "|" + (r.containerKey() != null ? r.containerKey() : r.cusip());
            JournalRecord cur = best.get(k);
            if (cur == null || r.version() > cur.version()) best.put(k, r);
        }
        Map<String, Map<String, JournalRecord>> cus = new HashMap<>();
        List<JournalRecord> positions = new ArrayList<>();
        for (JournalRecord r : best.values()) {
            if (r.source() == FeedSource.POSITION) positions.add(r);
            else cus.computeIfAbsent(r.cusip(), c -> new HashMap<>()).put(r.source().field(), r);
        }
        Map<String, String> out = new TreeMap<>();
        for (JournalRecord p : positions) {
            ObjectNode d = MAPPER.createObjectNode();
            boolean closed = p.value().path("closed").asBoolean(false);
            d.put("ck", p.containerKey());
            d.put("cusip", p.cusip());
            d.set("position", p.value());
            d.put("posVer", p.version());
            d.put("closed", closed);
            if (!closed) {
                boolean complete = true;
                Map<String, JournalRecord> fs = cus.getOrDefault(p.cusip(), Map.of());
                for (String f : FIELDS) {
                    JournalRecord v = fs.get(f);
                    d.set(f, v == null ? NullNode.getInstance() : v.value());
                    d.put(f + "Ver", v == null ? -1 : v.version());
                    complete &= v != null;
                }
                d.put("complete", complete);
            }
            out.put(p.containerKey(), canonical(d));
        }
        return out;
    }

    public static Map<String, String> canonical(Map<String, ObjectNode> views) {
        Map<String, String> out = new TreeMap<>();
        views.forEach((k, v) -> out.put(k, canonical(v)));
        return out;
    }

    public static Map<String, String> lastPerKey(List<ContainerUpdate> output) {
        Map<String, String> last = new TreeMap<>();
        for (ContainerUpdate u : output) last.put(u.containerKey(), canonical(parse(u.json())));
        return last;
    }

    public static boolean neverBackwards(List<ContainerUpdate> output) {
        Map<String, JsonNode> seen = new HashMap<>();
        for (ContainerUpdate u : output) {
            JsonNode v = parse(u.json());
            JsonNode p = seen.get(u.containerKey());
            if (p != null) {
                if (v.get("posVer").asLong() < p.get("posVer").asLong()) return false;
                for (String f : FIELDS) {
                    String n = f + "Ver";
                    if (v.has(n) && p.has(n) && v.get(n).asLong() < p.get(n).asLong()) return false;
                }
            }
            seen.put(u.containerKey(), v);
        }
        return true;
    }

    public static JsonNode parse(String json) {
        try {
            return MAPPER.readTree(json);
        } catch (Exception e) {
            throw new IllegalArgumentException(json, e);
        }
    }

    /** Sorted keys; numbers by value. */
    public static String canonical(JsonNode n) {
        StringBuilder sb = new StringBuilder();
        canonical(n, sb);
        return sb.toString();
    }

    private static void canonical(JsonNode n, StringBuilder sb) {
        if (n == null || n.isNull()) {
            sb.append("null");
        } else if (n.isObject()) {
            List<String> names = new ArrayList<>();
            for (String name : n.propertyNames()) names.add(name);
            Collections.sort(names);
            sb.append('{');
            for (int i = 0; i < names.size(); i++) {
                if (i > 0) sb.append(',');
                sb.append('"').append(names.get(i)).append("\":");
                canonical(n.get(names.get(i)), sb);
            }
            sb.append('}');
        } else if (n.isArray()) {
            sb.append('[');
            for (int i = 0; i < n.size(); i++) {
                if (i > 0) sb.append(',');
                canonical(n.get(i), sb);
            }
            sb.append(']');
        } else if (n.isNumber()) {
            sb.append(new BigDecimal(n.asString()).stripTrailingZeros().toPlainString());
        } else {
            sb.append(n.toString());
        }
    }
}
