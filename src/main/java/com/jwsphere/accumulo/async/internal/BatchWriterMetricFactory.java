package com.jwsphere.accumulo.async.internal;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

public class BatchWriterMetricFactory {

    private final long writerId;

    public BatchWriterMetricFactory(long writerId) {
        this.writerId = writerId;
    }

    public Timer registerOverallLatencyMetric(MeterRegistry registry) {
        return Timer.builder("ax.writer.write.latency")
                .description("The total write time for mutations.")
                .publishPercentileHistogram(true)
                .tag("writertype", "batchwriter")
                .tag("id", Long.toString(writerId))
                .register(registry);
    }

    public Timer registerSubmitLatencyMetric(MeterRegistry registry) {
        return Timer.builder("ax.writer.submit.latency")
                .description("The submit time for mutations.")
                .publishPercentileHistogram(true)
                .tag("writertype", "batchwriter")
                .tag("id", Long.toString(writerId))
                .register(registry);
    }

    public Timer registerWaitLatencyMetric(MeterRegistry registry) {
        return Timer.builder("ax.writer.wait.latency")
                .description("The wait time for mutations.  Does not include flush latency.")
                .publishPercentileHistogram(true)
                .tag("writertype", "batchwriter")
                .tag("id", Long.toString(writerId))
                .register(registry);
    }

    public Timer registerFlushLatencyMetric(MeterRegistry registry) {
        return Timer.builder("ax.writer.flush.latency")
                .description("The writer flush latency.")
                .publishPercentileHistogram(true)
                .tag("writertype", "batchwriter")
                .tag("id", Long.toString(writerId))
                .register(registry);
    }

    public void registerFailedMutationsMetric(MeterRegistry registry, AtomicLong value) {
        FunctionCounter.builder("ax.writer.failed", value, AtomicLong::get)
                .baseUnit("mutations")
                .description("The number of mutations completed exceptionally")
                .tag("writertype", "batchwriter")
                .tag("id", Long.toString(writerId))
                .register(registry);
    }

    public void registerSubmittedMutationsMetric(MeterRegistry registry, AtomicLong value) {
        FunctionCounter.builder("ax.writer.submitted", value, AtomicLong::get)
                .baseUnit("mutations")
                .description("The number of mutations submitted to the writer")
                .tag("writertype", "batchwriter")
                .tag("id", Long.toString(writerId))
                .register(registry);
    }

    public void registerFlushedMutationsMetric(MeterRegistry registry, AtomicLong value) {
        FunctionCounter.builder("ax.writer.flushed", value, AtomicLong::get)
                .baseUnit("mutations")
                .description("The number of mutations flushed to Accumulo")
                .tag("writertype", "batchwriter")
                .tag("id", Long.toString(writerId))
                .register(registry);
    }

    public void registerFlushOperationsMetric(MeterRegistry registry, AtomicLong value) {
        FunctionCounter.builder("ax.writer.flushes", value, AtomicLong::get)
                .baseUnit("flushes")
                .description("Counts the number of flushes")
                .tag("writertype", "batchwriter")
                .tag("id", Long.toString(writerId))
                .register(registry);
    }

    public void registerQueueDepthMetric(MeterRegistry registry, BlockingQueue<?> queue) {
        Gauge.builder("ax.writer.queuedepth", queue, BlockingQueue::size)
                .baseUnit("mutations")
                .description("An estimate of the number of mutations awaiting processing")
                .tag("writertype", "batchwriter")
                .tag("id", Long.toString(writerId))
                .register(registry);
    }

    public void registerBatchBytesMetric(MeterRegistry registry, AtomicLong bytes) {
        Gauge.builder("ax.writer.batchbytes", bytes, AtomicLong::get)
                .baseUnit("bytes")
                .description("The number of bytes of the latest batch")
                .tag("writertype", "batchwriter")
                .tag("id", Long.toString(writerId))
                .register(registry);
    }

}
