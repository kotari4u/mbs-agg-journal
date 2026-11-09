package com.example.mbsagg.journal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.example.mbsagg.journal.ShardState.CusipState;
import com.example.mbsagg.journal.StartPlan.Path;
import com.example.mbsagg.journal.redis.CheckpointStore;
import tools.jackson.databind.node.ObjectNode;

/**
 * The single writer for one journal partition and one business date. It applies journal records in offset order
 * in memory, so updates for a CUSIP never race. Per flush, the caller (see {@link Flusher}) first saves the changes
 * to Redis with {@link #checkpoint()}, then commits the output and the offset in one Kafka transaction.
 *
 * <p>Restart without seeking: Redis is written before Kafka, so its offset S is normally at or ahead of the
 * committed offset C. The new owner loads the snapshot and reads on from C. Records between C and S are already
 * in the snapshot, so they come back STALE, but the containers they touch are published again: their Kafka
 * transaction may never have committed. Only if Redis is behind Kafka does it replay from S.
 *
 * <p>Not thread-safe: one thread per worker.
 */
public final class ShardWorker {
    private final CheckpointStore store;
    private final String bizDate;
    private final int partition;
    private final Safeguards safeguards;
    private final ShardState state = new ShardState();
    private final Set<String> outDirty = new TreeSet<>();
    private final Set<String> slotsDirty = new TreeSet<>();
    private final Set<String> cusipsDirty = new TreeSet<>();
    private long gen;
    private long flushSeq;
    private long next = -1;
    private long silentUntil;
    private long republishUntil = -1;
    private long checkpointFrom = -1;
    private Path path;

    public ShardWorker(CheckpointStore store, String bizDate, int partition) {
        this(store, bizDate, partition, Safeguards.ALL);
    }

    ShardWorker(CheckpointStore store, String bizDate, int partition, Safeguards safeguards) {
        this.store = store;
        this.bizDate = bizDate;
        this.partition = partition;
        this.safeguards = safeguards;
    }

    public String bizDate() {
        return bizDate;
    }

    public int partition() {
        return partition;
    }

    public Path path() {
        return path;
    }

    /** The next journal offset to apply; -1 before the first record of a start-of-day replay. */
    public long nextOffset() {
        return next;
    }

    /** True while replaying records that were already published (nothing is published or checkpointed). */
    public boolean isReplaying() {
        return next < silentUntil;
    }

    /**
     * Takes ownership (fencing the previous owner in Redis) and decides where to start reading.
     *
     * @param committed C, the group's committed offset for this partition (or the start of day for a new group)
     */
    public StartPlan assign(long committed) {
        CheckpointStore.Claim claim = store.claim(bizDate, partition);
        gen = claim.gen();
        flushSeq = claim.flushSeq();
        long s = claim.offset();
        state.clear();
        outDirty.clear();
        slotsDirty.clear();
        cusipsDirty.clear();
        republishUntil = -1;
        checkpointFrom = -1;
        silentUntil = committed;
        if (s >= 0 && s >= committed) {
            store.load(bizDate, partition, state);
            if (safeguards.republishOnResume()) republishUntil = s;
            checkpointFrom = s;                       // Redis already holds everything up to S
            next = committed;
            path = Path.RESUME_AT_C;
        } else if (s >= 0 && safeguards.replayWhenRedisBehind()) {
            store.load(bizDate, partition, state);
            next = s;
            path = Path.REPLAY_FROM_SNAPSHOT;
        } else if (s >= 0) {                          // BUG variant only: trust a snapshot behind Kafka
            store.load(bizDate, partition, state);
            next = committed;
            path = Path.RESUME_AT_C;
        } else {
            next = -1;
            path = Path.REPLAY_FROM_START_OF_DAY;
        }
        return new StartPlan(path, committed, s);
    }

    /** Applies the record at {@code offset}. Records before C rebuild state silently; later ones are published. */
    public void apply(long offset, JournalRecord r) {
        boolean publish = offset >= silentUntil;
        CusipState cs = state.cusip(r.cusip());
        if (publish && offset < republishUntil) {
            // Between C and S: already in the snapshot, but maybe never published. Publish what it touches.
            if (r.source().isCusipGrain()) markOpenSlots(cs);
            else if (cs.slots.containsKey(r.containerKey())) outDirty.add(r.containerKey());
        }
        if (r.source().isCusipGrain()) {
            String f = r.source().field();
            if (newer(cs.fields.get(f), r.version())) {
                cs.fields.put(f, new Versioned(r.value(), r.version()));
                cusipsDirty.add(r.cusip());
                if (publish) markOpenSlots(cs);       // one-to-many: every open position of the CUSIP
            }
        } else {
            if (newer(cs.slots.get(r.containerKey()), r.version())) {
                cs.slots.put(r.containerKey(), new Versioned(r.value(), r.version()));
                slotsDirty.add(r.containerKey());
                if (publish) outDirty.add(r.containerKey());
            }
        }
        next = offset + 1;
    }

    /** A record from an earlier business date (a straggler): skipped, but its offset counts as read. */
    public void skip(long offset) {
        next = offset + 1;
    }

    private void markOpenSlots(CusipState cs) {
        cs.slots.forEach((ck, pos) -> {
            if (!ShardState.isClosed(pos)) outDirty.add(ck);
        });
    }

    private boolean newer(Versioned current, long version) {
        return !safeguards.versionCheck() || current == null || version > current.version();
    }

    /**
     * Step 1 of a flush: saves changed slots and CUSIPs with the new offset, atomically and fenced. Skipped while
     * replaying, and while re-reading C..S (Redis is already ahead). On a GAP (Redis lost writes) it rewrites the
     * whole shard.
     */
    public void checkpoint() {
        if (isReplaying() || next < checkpointFrom) return;
        Map<String, String> slots = new TreeMap<>();
        for (String ck : slotsDirty) slots.put(ck, state.slotJson(JournalRecord.cusipOf(ck), ck));
        Map<String, String> cus = new TreeMap<>();
        for (String c : cusipsDirty) cus.put(c, state.cusipJson(c));
        CheckpointStore.Result r = store.checkpoint(bizDate, partition, gen, flushSeq, next, slots, cus);
        switch (r.status()) {
            case OK -> {
                flushSeq = r.value();
                slotsDirty.clear();
                cusipsDirty.clear();
            }
            case FENCED -> throw new ShardFencedException("redis: shard " + partition + " has a newer owner (gen "
                    + r.value() + ")");
            case GAP -> resync();
        }
    }

    private void resync() {
        CheckpointStore.Result r = store.swap(bizDate, partition, gen, next, state.allSlotJson(), state.allCusipJson());
        if (r.status() == CheckpointStore.Status.FENCED) {
            throw new ShardFencedException("redis: shard " + partition + " has a newer owner (gen " + r.value() + ")");
        }
        flushSeq = r.value();
        slotsDirty.clear();
        cusipsDirty.clear();
    }

    /** Step 2 of a flush: the containers to publish in the Kafka transaction, with their current state. */
    public List<ContainerUpdate> pendingOutput() {
        List<ContainerUpdate> out = new ArrayList<>(outDirty.size());
        for (String ck : outDirty) out.add(new ContainerUpdate(ck, state.view(JournalRecord.cusipOf(ck), ck).toString()));
        return out;
    }

    /** Step 3 of a flush, after the Kafka commit. */
    public void committed() {
        outDirty.clear();
    }

    /** Every container as this worker holds it. */
    public Map<String, ObjectNode> views() {
        return state.views();
    }
}
