package com.jwsphere.accumulo.async;

import com.jwsphere.accumulo.async.internal.NoOpWriteListener;
import org.apache.accumulo.core.client.BatchWriterConfig;

import java.util.Objects;

public class AsyncMultiTableBatchWriterConfig {

    private final BatchWriterConfig config;

    private final long maxBytesPerFlush;

    private final double maxFlushRatePerSecond;

    private final AsyncMultiTableBatchWriter.ListenerFactory listenerFactory;

    private AsyncMultiTableBatchWriterConfig(BatchWriterConfig config,
                                             long maxBytesPerFlush,
                                             double maxFlushRatePerSecond,
                                             AsyncMultiTableBatchWriter.ListenerFactory listenerFactory) {
        this.config = Objects.requireNonNull(config);
        this.maxBytesPerFlush = maxBytesPerFlush;
        this.maxFlushRatePerSecond = maxFlushRatePerSecond;
        this.listenerFactory = Objects.requireNonNull(listenerFactory);
    }

    public AsyncMultiTableBatchWriterConfig withMaxBytesPerFlush(long maxBytesPerFlush) {
        if (maxBytesPerFlush <= 0) {
            throw new IllegalArgumentException("Limit on incomplete mutation size must be strictly positive.");
        }
        return new AsyncMultiTableBatchWriterConfig(config, maxBytesPerFlush, maxFlushRatePerSecond, listenerFactory);
    }


    public AsyncMultiTableBatchWriterConfig withMaxFlushRatePerSecond(double maxFlushRatePerSecond) {
        if (maxFlushRatePerSecond <= 0) {
            throw new IllegalArgumentException("Limit on flush rate must be strictly positive.");
        }
        return new AsyncMultiTableBatchWriterConfig(config, maxBytesPerFlush, maxFlushRatePerSecond, listenerFactory);
    }

    public AsyncMultiTableBatchWriterConfig withListener(AsyncMultiTableBatchWriter.ListenerFactory listenerFactory) {
        Objects.requireNonNull(listenerFactory);
        return new AsyncMultiTableBatchWriterConfig(config, maxBytesPerFlush, maxFlushRatePerSecond, listenerFactory);
    }

    public BatchWriterConfig getBatchWriterConfig() {
        return config;
    }

    public long getMaxBytesPerFlush() {
        return maxBytesPerFlush;
    }

    public double getMaxFlushRatePerSecond() {
        return maxFlushRatePerSecond;
    }

    public AsyncMultiTableBatchWriter.ListenerFactory getListenerFactory() {
        return listenerFactory;
    }

    public static AsyncMultiTableBatchWriterConfig create(BatchWriterConfig config) {
        return new AsyncMultiTableBatchWriterConfig(config, config.getMaxMemory(), 50, NoOpWriteListener.getInstance());
    }

}
