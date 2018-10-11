package com.jwsphere.accumulo.async.internal;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.data.Mutation;

public class NoopMultiTableBatchWriter implements MultiTableBatchWriter {

    private volatile boolean isClosed = false;

    @Override
    public BatchWriter getBatchWriter(String table) {
        return new NopBatchWriter();
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
        isClosed = true;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    private static class NopBatchWriter implements BatchWriter {

        @Override
        public void addMutation(Mutation m) {
        }

        @Override
        public void addMutations(Iterable<Mutation> iterable) {
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }

    }

}
