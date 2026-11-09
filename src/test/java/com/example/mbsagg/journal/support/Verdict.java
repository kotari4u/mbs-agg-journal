package com.example.mbsagg.journal.support;

/**
 * Checks for one simulated day.
 *
 * @param downstream the last output message per container equals the expected value
 * @param state      the final owner's in-memory state equals the expected value
 * @param monotonic  no container's output ever went back to an older version
 * @param snapshot   the Redis snapshot equals the final owner's state
 * @param fencedOk   every stale owner that was expected to be fenced was
 */
public record Verdict(boolean downstream, boolean state, boolean monotonic, boolean snapshot, boolean fencedOk) {
}
