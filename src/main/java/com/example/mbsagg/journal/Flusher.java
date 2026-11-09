package com.example.mbsagg.journal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * One flush, in the order that makes restarts safe without seeking: Redis first (state + offset S), then the Kafka
 * transaction (output + offset C). A crash between the two leaves Redis ahead of Kafka, which the next owner handles
 * by resuming at C and republishing what C..S touches.
 */
public final class Flusher {
    /** Points between the steps; the tests use them to crash. */
    public interface Hooks {
        Hooks NONE = new Hooks() {
        };

        default void afterRedis() {
        }

        default void afterCommit() {
        }
    }

    private Flusher() {
    }

    public static void flush(Collection<ShardWorker> workers, TransactionalSink sink) {
        flush(workers, sink, Hooks.NONE);
    }

    public static void flush(Collection<ShardWorker> workers, TransactionalSink sink, Hooks hooks) {
        for (ShardWorker w : workers) w.checkpoint();
        hooks.afterRedis();
        List<ContainerUpdate> output = new ArrayList<>();
        List<OffsetCommit> offsets = new ArrayList<>();
        for (ShardWorker w : workers) {
            if (w.isReplaying()) continue;            // committed offset stays at C until replay is done
            output.addAll(w.pendingOutput());
            offsets.add(new OffsetCommit(w.partition(), w.nextOffset(), w.bizDate()));
        }
        if (!offsets.isEmpty()) sink.commit(output, offsets);
        for (ShardWorker w : workers) w.committed();
        hooks.afterCommit();
    }
}
