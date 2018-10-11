package com.jwsphere.accumulo.async.internal;

public interface FlushEvent extends AutoCloseable {

    FlushEvent NO_OP = new FlushEvent() {};

    default void recordBatchEstimatedMemory(long numBytes) {
    }

    default void recordBatchSize(int numMutations) {
    }

    default void recordRemainingQueueDepth(int size) {
    }

    default void beforeRateLimiting() {
    }

    default void afterRateLimiting() {
    }

    default void beforeFlush() {
    }

    default void afterFlush() {
    }

    default void close() {
    }

}
