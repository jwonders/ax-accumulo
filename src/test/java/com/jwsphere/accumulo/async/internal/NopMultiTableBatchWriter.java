package com.jwsphere.accumulo.async.internal;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;

public class NopMultiTableBatchWriter implements MultiTableBatchWriter {

    private volatile boolean isClosed = false;

    @Override
    public BatchWriter getBatchWriter(String table) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        return new NopBatchWriter(table);
    }

    @Override
    public void flush() throws MutationsRejectedException {

    }

    @Override
    public void close() throws MutationsRejectedException {
        isClosed = true;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    private static class NopBatchWriter implements BatchWriter {

        private final String table;

        NopBatchWriter(String table) {
            this.table = table;
        }

        @Override
        public void addMutation(Mutation m) throws MutationsRejectedException {

        }

        @Override
        public void addMutations(Iterable<Mutation> iterable) throws MutationsRejectedException {

        }

        @Override
        public void flush() throws MutationsRejectedException {

        }

        @Override
        public void close() throws MutationsRejectedException {

        }

    }

}
