package com.jwsphere.accumulo.async;

import com.jwsphere.accumulo.async.internal.AsyncConditionalWriterImpl;
import com.jwsphere.accumulo.async.internal.AsyncMultiTableBatchWriterImpl;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

public class AsyncConnector {

    private Connector connector;
    private ExecutorService executor;

    public AsyncConnector(Connector connector) {
        this.connector = connector;
        this.executor = ForkJoinPool.commonPool();
    }

    public AsyncConnector(Connector connector, ExecutorService executor) {
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

    public AsyncConditionalWriter createConditionalWriter(String tableName, ConditionalWriterConfig config) throws TableNotFoundException {
        return new AsyncConditionalWriterImpl(connector.createConditionalWriter(tableName, config));
    }

    public AsyncMultiTableBatchWriter createMultiTableBatchWriter(BatchWriterConfig config) {
        return new AsyncMultiTableBatchWriterImpl(connector.createMultiTableBatchWriter(config));
    }
}
