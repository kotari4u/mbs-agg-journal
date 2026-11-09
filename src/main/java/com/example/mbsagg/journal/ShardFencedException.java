package com.example.mbsagg.journal;

/** A newer owner holds this shard (Redis owner generation) or this group generation (Kafka). Stop this worker. */
public final class ShardFencedException extends RuntimeException {
    public ShardFencedException(String message) {
        super(message);
    }

    public ShardFencedException(String message, Throwable cause) {
        super(message, cause);
    }
}
