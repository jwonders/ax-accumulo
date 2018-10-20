package com.jwsphere.accumulo.async.internal;

import com.jwsphere.accumulo.async.AsyncMultiTableBatchWriter;

public class NoOpWriteListener implements AsyncMultiTableBatchWriter.Listener {

    private static final AsyncMultiTableBatchWriter.FlushEvent NO_OP_FLUSH_EVENT = new NoOpFlushEvent();
    private static final AsyncMultiTableBatchWriter.Listener NO_OP_LISTENER = new NoOpWriteListener();
    private static final AsyncMultiTableBatchWriter.ListenerFactory NO_OP_LISTENER_FACTORY = id -> NO_OP_LISTENER;

    @Override
    public AsyncMultiTableBatchWriter.FlushEvent startFlushEvent() {
        return NO_OP_FLUSH_EVENT;
    }

    private static class NoOpFlushEvent implements AsyncMultiTableBatchWriter.FlushEvent {
    }

    public static AsyncMultiTableBatchWriter.ListenerFactory getInstance() {
        return NO_OP_LISTENER_FACTORY;
    }

}
