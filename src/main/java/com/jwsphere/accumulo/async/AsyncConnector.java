package com.jwsphere.accumulo.async;

import com.jwsphere.accumulo.async.internal.AsyncConditionalWriterImpl;
import com.jwsphere.accumulo.async.internal.AsyncMultiTableBatchWriterImpl;
import com.jwsphere.accumulo.async.internal.BoundedAsyncConditionalWriter;
import com.jwsphere.accumulo.async.internal.LimitedCapacityAsyncConditionalWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class AsyncConnector {

    private Connector connector;
    private Executor executor;

    public AsyncConnector(Connector connector) {
        this.connector = connector;
        this.executor = ForkJoinPool.commonPool();
    }

    public AsyncConnector(Connector connector, Executor executor) {
        this.connector = connector;
        this.executor = executor;
    }

    public Connector getConnector() {
        return connector;
    }

    public AsyncTableOperations tableOperations() {
        return new AsyncTableOperations(connector.tableOperations(), executor);
    }

    public AsyncSecurityOperations securityOperations() {
        return new AsyncSecurityOperations(connector.securityOperations(), executor);
    }

    public AsyncNamespaceOperations namespaceOperations() {
        return new AsyncNamespaceOperations(connector.namespaceOperations(), executor);
    }

    public AsyncConditionalWriter createConditionalWriter(String tableName, AsyncConditionalWriterConfig config) throws TableNotFoundException {
        ConditionalWriter cw = connector.createConditionalWriter(tableName, config.getConditionalWriterConfig());

        final AsyncConditionalWriter writer = new AsyncConditionalWriterImpl(cw);

        final AsyncConditionalWriter boundedWriter = config.getIncompleteMutationsLimit()
                .map(createBoundedWriter(writer))
                .orElse(writer);

        return config.getMemoryCapacityLimit()
                .map(createCapacityLimitedWriter(boundedWriter))
                .orElse(boundedWriter);
    }

    private Function<Long, AsyncConditionalWriter> createCapacityLimitedWriter(AsyncConditionalWriter boundedWriter) {
        return limit -> new LimitedCapacityAsyncConditionalWriter(boundedWriter, limit);
    }

    private Function<Integer, AsyncConditionalWriter> createBoundedWriter(AsyncConditionalWriter writer) {
        return limit -> new BoundedAsyncConditionalWriter(writer, limit);
    }

    public AsyncMultiTableBatchWriter createMultiTableBatchWriter(BatchWriterConfig config) {
        return new AsyncMultiTableBatchWriterImpl(connector.createMultiTableBatchWriter(config));
    }
}
