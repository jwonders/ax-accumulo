package com.jwsphere.accumulo.async;

import com.jwsphere.accumulo.async.internal.AsyncConditionalWriterImpl;
import com.jwsphere.accumulo.async.internal.AsyncMultiTableBatchWriterImpl;
import com.jwsphere.accumulo.async.internal.ScanBuilderImpl;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;

import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

/**
 * A wrapper around an Accumulo {@link Connector} that executes blocking
 * operations asynchronously and exposes an asynchronous API.
 *
 * @author Jonathan Wonders
 */
public class AsyncConnector {

    private final Connector connector;
    private final Executor executor;

    private AsyncConnector(Connector connector, Executor executor) {
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
        return new AsyncConditionalWriterImpl(connector, tableName, config);
    }

    public AsyncMultiTableBatchWriter createMultiTableBatchWriter(BatchWriterConfig config) {
        return new AsyncMultiTableBatchWriterImpl(
                () -> connector.createMultiTableBatchWriter(config),
                AsyncMultiTableBatchWriterConfig.create(config)
        );
    }

    public AsyncMultiTableBatchWriter createMultiTableBatchWriter(AsyncMultiTableBatchWriterConfig config) {
        return new AsyncMultiTableBatchWriterImpl(
                () -> connector.createMultiTableBatchWriter(config.getBatchWriterConfig()), config);
    }

    public ScanBuilder createScanBuilder(String table) {
        return new ScanBuilderImpl(connector, table, executor);
    }

    public ScanBuilder createScanBuilder(String table, Executor executor) {
        return new ScanBuilderImpl(connector, table, executor);
    }

    public static AsyncConnector wrap(Connector connector) {
        return wrap(connector, ForkJoinPool.commonPool());
    }

    public static AsyncConnector wrap(Connector connector, Executor defaultExecutor) {
        return new AsyncConnector(connector, defaultExecutor);
    }

}
