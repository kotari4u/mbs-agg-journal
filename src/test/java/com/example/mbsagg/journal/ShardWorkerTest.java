package com.example.mbsagg.journal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.example.mbsagg.journal.redis.CheckpointStore;
import com.example.mbsagg.journal.support.TestRedis;
import com.example.mbsagg.journal.support.Views;
import tools.jackson.databind.json.JsonMapper;

class ShardWorkerTest {
    private static final String D = "2026-11-09";
    private static final JsonMapper M = JsonMapper.shared();
    private static TestRedis redis;
    private CheckpointStore store;
    private final List<ContainerUpdate> published = new ArrayList<>();
    private long committed;
    private final TransactionalSink sink = (out, offsets) -> {
        published.addAll(out);
        committed = offsets.get(0).nextOffset();
    };

    @BeforeAll
    static void startRedis() {
        redis = TestRedis.get();
    }

    @BeforeEach
    void reset() {
        redis.reset();
        store = new CheckpointStore(redis.template);
        published.clear();
        committed = 0;
    }

    private static JournalRecord price(long ver, double px) {
        return new JournalRecord(FeedSource.PRICE, D, "C1", null, ver, M.createObjectNode().put("px", px));
    }

    private static JournalRecord position(String inventory, long ver, long face) {
        return new JournalRecord(FeedSource.POSITION, D, "C1", JournalRecord.containerKey("P1", inventory, "C1"), ver,
                M.createObjectNode().put("face", face).put("closed", false));
    }

    private static Set<String> keys(List<ContainerUpdate> us) {
        Set<String> s = new TreeSet<>();
        for (ContainerUpdate u : us) s.add(u.containerKey());
        return s;
    }

    @Test
    void onePriceUpdatesEveryContainerOfTheCusip() {
        ShardWorker w = new ShardWorker(store, D, 0);
        w.assign(0);
        w.apply(0, position("INV1", 0, 1000));
        w.apply(1, position("INV2", 1, 2000));
        w.apply(2, position("INV3", 2, 3000));
        Flusher.flush(List.of(w), sink);
        published.clear();
        w.apply(3, price(0, 99.5));
        Flusher.flush(List.of(w), sink);
        assertEquals(Set.of("P1|INV1|C1", "P1|INV2|C1", "P1|INV3|C1"), keys(published));
        for (ContainerUpdate u : published) assertEquals(99.5, Views.parse(u.json()).get("price").get("px").asDouble());
        assertEquals(4, committed);
    }

    @Test
    void aRepeatedOrOlderRecordChangesNothing() {
        ShardWorker w = new ShardWorker(store, D, 0);
        w.assign(0);
        w.apply(0, position("INV1", 0, 1000));
        w.apply(1, price(5, 99.5));
        w.apply(2, price(5, 99.5));          // sequencer re-send
        w.apply(3, price(4, 98.0));          // older
        assertEquals(99.5, w.views().get("P1|INV1|C1").get("price").get("px").asDouble());
        assertEquals(5, w.views().get("P1|INV1|C1").get("priceVer").asLong());
    }

    @Test
    void restartResumesAtCAndRepublishesWhatCToSTouches() {
        // Owner A: flushes up to offset 2, then writes Redis up to offset 4 and dies before its Kafka commit.
        ShardWorker a = new ShardWorker(store, D, 0);
        a.assign(0);
        a.apply(0, position("INV1", 0, 1000));
        a.apply(1, position("INV2", 1, 2000));
        Flusher.flush(List.of(a), sink);
        a.apply(2, price(0, 99.5));
        a.apply(3, position("INV1", 2, 1500));
        a.checkpoint();                      // Redis now at S = 4; Kafka still at C = 2
        assertEquals(2, committed);
        published.clear();

        ShardWorker b = new ShardWorker(store, D, 0);
        StartPlan plan = b.assign(committed);
        assertEquals(StartPlan.Path.RESUME_AT_C, plan.path());
        assertEquals(2, plan.committed());
        assertEquals(4, plan.snapshotOffset());
        b.apply(2, price(0, 99.5));          // re-read: STALE against the snapshot, but republished
        b.apply(3, position("INV1", 2, 1500));
        Flusher.flush(List.of(b), sink);
        assertEquals(Set.of("P1|INV1|C1", "P1|INV2|C1"), keys(published), "A's lost output goes out after all");
        assertEquals(4, committed);
    }

    @Test
    void anOldOwnerIsFencedInRedis() {
        ShardWorker a = new ShardWorker(store, D, 0);
        a.assign(0);
        a.apply(0, position("INV1", 0, 1000));
        ShardWorker b = new ShardWorker(store, D, 0);
        b.assign(0);
        try {
            a.checkpoint();
            fail("expected the old owner to be fenced");
        } catch (ShardFencedException expected) {
            // b's claim moved the owner generation
        }
    }

    @Test
    void lostRedisWritesAreDetectedAndTheShardIsRewritten() {
        ShardWorker w = new ShardWorker(store, D, 0);
        w.assign(0);
        w.apply(0, position("INV1", 0, 1000));
        w.checkpoint();
        redis.template.opsForHash().put(CheckpointStore.meta(D, 0), "flushSeq", "0");   // as if the last checkpoint was lost
        redis.template.delete(CheckpointStore.slots(D, 0));
        w.apply(1, position("INV2", 1, 2000));
        w.checkpoint();                      // GAP: rewrites the whole shard
        ShardState snapshot = new ShardState();
        store.load(D, 0, snapshot);
        assertEquals(Views.canonical(w.views()), Views.canonical(snapshot.views()));
        assertTrue(snapshot.views().containsKey("P1|INV1|C1"));
    }
}
