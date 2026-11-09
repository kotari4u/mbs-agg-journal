package com.example.mbsagg.journal;

import java.util.List;

/**
 * Commits a flush's output and offsets atomically: one Kafka transaction with
 * {@code sendOffsetsToTransaction(groupMetadata)}. Throws {@link ShardFencedException} if this owner was fenced.
 */
@FunctionalInterface
public interface TransactionalSink {
    void commit(List<ContainerUpdate> output, List<OffsetCommit> offsets);
}
