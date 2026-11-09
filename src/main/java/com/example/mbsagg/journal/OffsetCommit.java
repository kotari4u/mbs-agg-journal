package com.example.mbsagg.journal;

/**
 * The offset to commit for one journal partition, with the business date as offset metadata (a restarted owner
 * reads it to pick the right day's snapshot).
 */
public record OffsetCommit(int partition, long nextOffset, String bizDate) {
}
