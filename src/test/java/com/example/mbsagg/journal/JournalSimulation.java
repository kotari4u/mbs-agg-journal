package com.example.mbsagg.journal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import java.util.concurrent.TimeUnit;

import com.example.mbsagg.journal.redis.CheckpointStore;
import com.example.mbsagg.journal.support.SimJournal;
import com.example.mbsagg.journal.support.TestRedis;
import com.example.mbsagg.journal.support.Verdict;
import com.example.mbsagg.journal.support.Views;

import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * The simulation behind the scenario tests: the production {@link ShardWorker} and {@link Flusher} against a real
 * Redis, with Kafka modelled by {@link Broker}. One journal partition. Ported from {@code sim_resume_c.py}
 * (republish mode).
 */
final class JournalSimulation {
    static final String DATE = SimJournal.DATE;

    private JournalSimulation() {
    }

    static int seeds() {
        return Integer.getInteger("mbsagg.seeds", 40);
    }

    /** Stands in for Kafka: committed offset, group generation (bumped on every assignment), committed output. */
    static final class Broker {
        long committed;
        int generation;
        final List<ContainerUpdate> output = new ArrayList<>();
    }

    static final class Crash extends RuntimeException {
        Crash() {
            super("injected crash", null, false, false);
        }
    }

    /** One owner of the partition, driving a {@link ShardWorker} the way {@code AggregatorLoop} does. */
    static final class Owner {
        final Broker b;
        final CheckpointStore store;
        final List<JournalRecord> j;
        final int every;
        final Safeguards safeguards;
        ShardWorker w;
        int kgen;
        long next;
        StartPlan plan;
        final TransactionalSink sink;

        Owner(Broker b, CheckpointStore store, List<JournalRecord> j, int every, Safeguards safeguards) {
            this.b = b;
            this.store = store;
            this.j = j;
            this.every = every;
            this.safeguards = safeguards;
            this.sink = (output, offsets) -> {
                if (b.generation != kgen) throw new ShardFencedException("kafka: group generation moved");
                b.output.addAll(output);
                b.committed = offsets.get(0).nextOffset();
            };
        }

        /** Assignment: claim, choose the start, replay up to C silently if needed (what the loop's seek does). */
        Owner assign() {
            kgen = ++b.generation;
            w = new ShardWorker(store, DATE, 0, safeguards);
            long c = b.committed;
            plan = w.assign(c);
            long seek = switch (plan.path()) {
                case RESUME_AT_C -> c;
                case REPLAY_FROM_SNAPSHOT -> plan.snapshotOffset();
                case REPLAY_FROM_START_OF_DAY -> 0;
            };
            for (long off = seek; off < c; off++) w.apply(off, j.get((int) off));
            next = c;
            return this;
        }

        void applyNext() {
            w.apply(next, j.get((int) next));
            next++;
        }

        void publish(String crash) {
            Flusher.flush(List.of(w), sink, new Flusher.Hooks() {
                @Override
                public void afterRedis() {
                    if ("afterRedis".equals(crash)) throw new Crash();
                }

                @Override
                public void afterCommit() {
                    if ("afterKafka".equals(crash)) throw new Crash();
                }
            });
        }

        /** Step 1 alone (zombie scenarios). */
        void redisCheckpoint() {
            w.checkpoint();
        }

        /** Step 2 alone (zombie scenarios). */
        void kafkaCommit() {
            sink.commit(w.pendingOutput(), List.of(new OffsetCommit(0, w.nextOffset(), DATE)));
            w.committed();
        }

        Owner run(int n, Random rnd) {
            long end = n < 0 ? j.size() : Math.min(j.size(), next + n);
            while (next < end) {
                applyNext();
                if (next % every == 0) {
                    String crash = null;
                    if (rnd != null) {
                        double x = rnd.nextDouble();
                        crash = x < 0.06 ? "afterRedis" : x < 0.12 ? "afterKafka" : null;
                    }
                    publish(crash);
                }
            }
            if (next >= j.size()) publish(null);
            return this;
        }

        Owner runToEnd() {
            return run(-1, null);
        }
    }

    private static Verdict verdict(TestRedis redis, Broker b, Owner owner, List<JournalRecord> j, boolean fencedOk) {
        Map<String, String> expected = Views.expected(j);
        ShardState snapshot = new ShardState();
        new CheckpointStore(redis.template).load(DATE, 0, snapshot);
        return new Verdict(Views.lastPerKey(b.output).equals(expected),
                Views.canonical(owner.w.views()).equals(expected), Views.neverBackwards(b.output),
                Views.canonical(snapshot.views()).equals(expected), fencedOk);
    }

    // ------------------------------------------------------------------ scenarios

    /** Random crashes after the Redis write or after the Kafka commit, and random restarts. */
    static Verdict crashes(TestRedis redis, long seed, double dupRate, Safeguards sg) {
        Random rnd = new Random(seed);
        List<JournalRecord> j = SimJournal.day(seed, dupRate);
        Broker b = new Broker();
        CheckpointStore store = new CheckpointStore(redis.template);
        int[] choices = {10, 25, 60};
        int every = choices[rnd.nextInt(3)];
        Owner w = new Owner(b, store, j, every, sg).assign();
        while (true) {
            boolean crashed = false;
            try {
                w.run(1 + rnd.nextInt(500), rnd);
            } catch (Crash e) {
                crashed = true;
            }
            if (w.next >= j.size() && !crashed) break;
            if (crashed || rnd.nextDouble() < 0.6) w = new Owner(b, store, j, every, sg).assign();
        }
        return verdict(redis, b, w, j, true);
    }

    /**
     * An old owner A stalls (after or before its Redis write); the partition moves to B, which resumes at C.
     * A wakes and tries to write Redis and commit Kafka: both must be fenced.
     */
    static Verdict zombie(TestRedis redis, long seed, boolean stallAfterRedis) {
        Random rnd = new Random(seed);
        List<JournalRecord> j = SimJournal.day(seed, 0.03);
        Broker b = new Broker();
        CheckpointStore store = new CheckpointStore(redis.template);
        Owner a = new Owner(b, store, j, 25, Safeguards.ALL).assign();
        a.run(j.size() / 3, null);
        int more = 5 + rnd.nextInt(16);
        for (int i = 0; i < more; i++) a.applyNext();
        List<Boolean> fenced = new ArrayList<>();
        if (stallAfterRedis) a.redisCheckpoint();             // A saved state, then stalls before Kafka
        Owner nb = new Owner(b, store, j, 25, Safeguards.ALL).assign();
        nb.run(50 + rnd.nextInt(251), null);
        if (!stallAfterRedis) fenced.add(fencedOn(a::redisCheckpoint));
        fenced.add(fencedOn(a::kafkaCommit));
        fenced.add(fencedOn(() -> {
            for (int i = 0; i < 10; i++) a.applyNext();
            a.redisCheckpoint();
        }));
        nb.runToEnd();
        return verdict(redis, b, nb, j, fenced.stream().allMatch(x -> x));
    }

    private static boolean fencedOn(Runnable r) {
        try {
            r.run();
            return false;
        } catch (ShardFencedException e) {
            return true;
        }
    }

    /** Redis loses its recent checkpoints (failover to a lagging replica): Redis is now behind Kafka. */
    static Verdict redisLoss(TestRedis redis, long seed, Safeguards sg) {
        Random rnd = new Random(seed);
        List<JournalRecord> j = SimJournal.day(seed, 0.03);
        Broker b = new Broker();
        CheckpointStore store = new CheckpointStore(redis.template);
        Owner w = new Owner(b, store, j, 40, sg).assign().run(j.size() / 3, null);
        List<String> keys = List.of(CheckpointStore.meta(DATE, 0), CheckpointStore.slots(DATE, 0),
                CheckpointStore.cusips(DATE, 0));
        StringRedisTemplate t = redis.template;
        Map<String, byte[]> snap = new HashMap<>();
        for (String k : keys) snap.put(k, Boolean.TRUE.equals(t.hasKey(k)) ? t.dump(k) : null);
        w.run(100 + rnd.nextInt(301), null);
        for (String k : keys) {
            if (snap.get(k) == null) t.delete(k);
            else t.restore(k, snap.get(k), 0, TimeUnit.MILLISECONDS, true);
        }
        if (rnd.nextDouble() < 0.5) w = new Owner(b, store, j, 40, sg).assign();   // restart while Redis is behind
        w.runToEnd();
        return verdict(redis, b, w, j, true);
    }

    /** Redis wiped mid-day: the next owner replays from the start of the day. */
    static Verdict redisWiped(TestRedis redis, long seed) {
        List<JournalRecord> j = SimJournal.day(seed, 0.03);
        Broker b = new Broker();
        CheckpointStore store = new CheckpointStore(redis.template);
        new Owner(b, store, j, 40, Safeguards.ALL).assign().run(j.size() / 2, null);
        redis.reset();
        Owner w = new Owner(b, store, j, 40, Safeguards.ALL).assign().runToEnd();
        return verdict(redis, b, w, j, true);
    }
}
