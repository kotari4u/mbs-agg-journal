package com.example.mbsagg.journal;

import java.util.Map;
import java.util.TreeMap;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.NullNode;
import tools.jackson.databind.node.ObjectNode;

/**
 * In-memory state of one journal partition for one business date. CUSIP-grain fields are stored once per CUSIP;
 * a container is its position plus its CUSIP's fields, built when published.
 */
public final class ShardState {
    static final JsonMapper MAPPER = JsonMapper.shared();
    private static final String[] FIELDS = {"price", "risk", "assumptions"};

    /** One CUSIP: its price, risk and assumptions, and the positions (slots) that hold it. */
    public static final class CusipState {
        final Map<String, Versioned> fields = new TreeMap<>();
        final Map<String, Versioned> slots = new TreeMap<>();
    }

    private final Map<String, CusipState> cusips = new TreeMap<>();

    CusipState cusip(String cusip) {
        return cusips.computeIfAbsent(cusip, c -> new CusipState());
    }

    void clear() {
        cusips.clear();
    }

    static boolean isClosed(Versioned position) {
        return position != null && position.value().isObject() && position.value().path("closed").asBoolean(false);
    }

    /** The container view published downstream. */
    public ObjectNode view(String cusip, String containerKey) {
        CusipState cs = cusips.get(cusip);
        Versioned pos = cs == null ? null : cs.slots.get(containerKey);
        ObjectNode d = MAPPER.createObjectNode();
        boolean closed = isClosed(pos);
        d.put("ck", containerKey);
        d.put("cusip", cusip);
        d.set("position", pos == null ? NullNode.getInstance() : pos.value());
        d.put("posVer", pos == null ? -1 : pos.version());
        d.put("closed", closed);
        if (!closed) {
            boolean complete = pos != null;
            for (String f : FIELDS) {
                Versioned v = cs == null ? null : cs.fields.get(f);
                d.set(f, v == null ? NullNode.getInstance() : v.value());
                d.put(f + "Ver", v == null ? -1 : v.version());
                complete &= v != null;
            }
            d.put("complete", complete);
        }
        return d;
    }

    /** Every container, keyed by container key. */
    public Map<String, ObjectNode> views() {
        Map<String, ObjectNode> out = new TreeMap<>();
        cusips.forEach((c, cs) -> cs.slots.keySet().forEach(ck -> out.put(ck, view(c, ck))));
        return out;
    }

    // ------------------------------------------------------------------ snapshot format

    String slotJson(String cusip, String containerKey) {
        Versioned pos = cusips.get(cusip).slots.get(containerKey);
        ObjectNode n = MAPPER.createObjectNode();
        n.put("cusip", cusip);
        n.set("position", pos.value());
        n.put("posVer", pos.version());
        return n.toString();
    }

    String cusipJson(String cusip) {
        ObjectNode n = MAPPER.createObjectNode();
        cusips.get(cusip).fields.forEach((f, v) -> {
            ObjectNode fv = n.putObject(f);
            fv.set("value", v.value());
            fv.put("version", v.version());
        });
        return n.toString();
    }

    /** Every slot, as snapshot JSON. */
    Map<String, String> allSlotJson() {
        Map<String, String> m = new TreeMap<>();
        cusips.forEach((c, cs) -> cs.slots.keySet().forEach(ck -> m.put(ck, slotJson(c, ck))));
        return m;
    }

    /** Every CUSIP with at least one field, as snapshot JSON. */
    Map<String, String> allCusipJson() {
        Map<String, String> m = new TreeMap<>();
        cusips.forEach((c, cs) -> {
            if (!cs.fields.isEmpty()) m.put(c, cusipJson(c));
        });
        return m;
    }

    public void loadSlot(String containerKey, String json) {
        JsonNode n = parse(json);
        cusip(n.get("cusip").asString()).slots.put(containerKey, new Versioned(n.get("position"), n.get("posVer").asLong()));
    }

    public void loadCusip(String cusip, String json) {
        CusipState cs = cusip(cusip);
        parse(json).properties().forEach(e ->
                cs.fields.put(e.getKey(), new Versioned(e.getValue().get("value"), e.getValue().get("version").asLong())));
    }

    private static JsonNode parse(String json) {
        try {
            return MAPPER.readTree(json);
        } catch (Exception e) {
            throw new IllegalStateException("bad snapshot JSON: " + json, e);
        }
    }
}
