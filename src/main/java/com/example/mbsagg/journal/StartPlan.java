package com.example.mbsagg.journal;

/**
 * Where a newly assigned shard starts reading.
 *
 * @param path           which restart path applies
 * @param committed      C: the Kafka committed offset; records before it were already published
 * @param snapshotOffset S: the next offset the Redis snapshot covers, or -1 if there is none
 */
public record StartPlan(Path path, long committed, long snapshotOffset) {
    public enum Path {
        /**
         * Normal: Redis is at or ahead of Kafka (S >= C). Load the snapshot and resume reading at C, with no seek.
         * Records between C and S come back STALE, and the containers they touch are published again.
         */
        RESUME_AT_C,
        /** Rare: Redis lost writes and is behind Kafka (S < C). Load the snapshot, replay S..C silently. */
        REPLAY_FROM_SNAPSHOT,
        /** No snapshot for this date: replay from the start of the business day up to C silently. */
        REPLAY_FROM_START_OF_DAY
    }
}
