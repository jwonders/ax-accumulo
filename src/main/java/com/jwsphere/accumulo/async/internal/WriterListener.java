package com.jwsphere.accumulo.async.internal;

import java.util.concurrent.TimeUnit;

public interface WriterListener {

    default FlushEvent startFlushEvent() {
        return FlushEvent.NO_OP;
    }

    default void recordSubmission(long writerId) {
    }

    default void recordWaitTime(long writerId, long duration, TimeUnit unit) {
    }

    default void recordSubmitTime(long writerId, long duration, TimeUnit unit) {
    }

    default void recordFlushLatency(long writerId, long duration, TimeUnit unit) {
    }

    default void recordWriteLatency(long writerId, long duration, TimeUnit unit) {
    }

    default void recordSubmissionTimedOut(long writerId) {
    }

    default void recordFlush(long writerId) {
    }

}
