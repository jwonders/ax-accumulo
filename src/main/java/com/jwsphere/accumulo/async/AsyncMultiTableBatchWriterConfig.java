package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.client.BatchWriterConfig;

public class AsyncMultiTableBatchWriterConfig {

    private final BatchWriterConfig config;

    private final long maxBytesPerFlush;

    private final double maxFlushRatePerSecond;

    private AsyncMultiTableBatchWriterConfig(BatchWriterConfig config, long maxBytesPerFlush, double maxFlushRatePerSecond) {
        this.config = config;
        this.maxBytesPerFlush = maxBytesPerFlush;
        this.maxFlushRatePerSecond = maxFlushRatePerSecond;
    }

    public AsyncMultiTableBatchWriterConfig withMaxBytesPerFlush(long maxBytesPerFlush) {
        if (maxBytesPerFlush <= 0) {
            throw new IllegalArgumentException("Limit on incomplete mutation size must be strictly positive.");
        }
        return new AsyncMultiTableBatchWriterConfig(config, maxBytesPerFlush, maxFlushRatePerSecond);
    }


    public AsyncMultiTableBatchWriterConfig withMaxFlushRatePerSecond(double maxFlushRatePerSecond) {
        if (maxFlushRatePerSecond <= 0) {
            throw new IllegalArgumentException("Limit on flush rate must be strictly positive.");
        }
        return new AsyncMultiTableBatchWriterConfig(config, maxBytesPerFlush, maxFlushRatePerSecond);
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

    public static AsyncMultiTableBatchWriterConfig create(BatchWriterConfig config) {
        return new AsyncMultiTableBatchWriterConfig(config, config.getMaxMemory(), 50);
    }

}
