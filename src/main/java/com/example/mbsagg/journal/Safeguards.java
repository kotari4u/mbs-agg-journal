package com.example.mbsagg.journal;

/**
 * The worker's safeguards. Production always uses {@link #ALL}; the tests switch one off at a time to show that the
 * scenario checks catch its absence.
 */
record Safeguards(boolean versionCheck, boolean republishOnResume, boolean replayWhenRedisBehind) {
    static final Safeguards ALL = new Safeguards(true, true, true);
}
