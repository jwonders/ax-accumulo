package com.jwsphere.accumulo.async.internal;

import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jwsphere.accumulo.async.AsyncConditionalWriter;
import com.jwsphere.accumulo.async.AsyncConditionalWriterConfig;
import com.jwsphere.accumulo.async.CapacityExceededException;
import com.jwsphere.accumulo.async.FailurePolicy;
import com.jwsphere.accumulo.async.ResultBatchException;
import com.jwsphere.accumulo.async.ResultException;
import com.jwsphere.accumulo.async.SubmissionTimeoutException;
import com.jwsphere.accumulo.async.internal.Interruptible.InterruptibleFunction;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ConditionalMutation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.jwsphere.accumulo.async.internal.Interruptible.propagateInterrupt;
import static com.jwsphere.accumulo.async.internal.MoreCompletableFutures.propagateResultTo;

/**
 * The asynchronous conditional writer allows a bounded set of mutations to be
 * in the process of being written to Accumulo and informs the submitter upon
 * completion or error.
 *
 * The number of in-flight mutations needs to be bounded since the underlying
 * conditional writer uses an unbounded queue and does not protect against out
 * of memory errors.  Mutations are submitted to the underlying writer in the
 * caller's thread since the writer's internal blocking queue is unbounded and
 * should never block.  This assumption is important for the non-blocking methods
 * to exhibit proper behavior (e.g. ${code trySubmit}, ${code trySubmitMany})
 * since the writer does not otherwise have a non-blocking submission method.
 *
 * Note: The conditional writer does incur some per-mutation overhead due to
 * copying and retaining in-flight mutations in a queue.  The capacity limit
 * should be much smaller the the JVM heap and be sized based on how much data
 * needs to be batched to get acceptable throughput.  Higher capacities will
 * likely incur higher latencies when saturated.
 *
 * @author Jonathan Wonders
 */
public final class AsyncConditionalWriterImpl implements AsyncConditionalWriter {

    private static final SubmissionTimeoutException SUBMISSION_TIMEOUT = new SubmissionTimeoutException();

    private static final AtomicLong ID = new AtomicLong();

    private final ConditionalWriter writer;
    private final CompletionBarrier barrier;

    private final ExecutorService completionExecutor;

    private final long id = ID.getAndIncrement();

    private final long capacityLimit;
    private final LongSemaphore capacityLimiter;
    private final RateLimiter rateLimiter;
    private final FailurePolicy failurePolicy;

    private final MeterRegistry registry;

    /**
     * Creates an async conditional writer.
     */
    public AsyncConditionalWriterImpl(Connector connector, String tableName, AsyncConditionalWriterConfig config) throws TableNotFoundException {
        this(connector, tableName, config, new SimpleMeterRegistry());
    }

    /**
     * Creates an async conditional writer.
     */
    public AsyncConditionalWriterImpl(Connector connector, String tableName, AsyncConditionalWriterConfig config, MeterRegistry registry) throws TableNotFoundException {
        this(connector.createConditionalWriter(tableName, config.getConditionalWriterConfig()),null,
                config.getMemoryCapacityLimit().orElse(Long.MAX_VALUE),
                new LongSemaphore(config.getMemoryCapacityLimit().orElse(Long.MAX_VALUE)),
                null, FailurePolicy.allNormal(), registry);
    }

    /**
     * Creates an async conditional writer.
     */
    private AsyncConditionalWriterImpl(ConditionalWriter writer, ExecutorService completionExecutor,
                                       long capacityLimit, LongSemaphore capacityLimiter,
                                       RateLimiter rateLimiter, FailurePolicy failurePolicy,
                                       MeterRegistry registry) {
        this.writer = writer;
        this.barrier = new CompletionBarrier();
        this.completionExecutor = completionExecutor == null ? defaultExecutor() : completionExecutor;
        this.capacityLimit = capacityLimit;
        this.capacityLimiter = capacityLimiter;
        this.rateLimiter = rateLimiter;
        this.failurePolicy = failurePolicy;
        this.registry = registry;
    }

    @Override
    public AsyncConditionalWriter withRateLimit(double bytesPerSecond) {
        RateLimiter rateLimiter = RateLimiter.create(bytesPerSecond);
        return new AsyncConditionalWriterImpl(writer, completionExecutor, capacityLimit, capacityLimiter, rateLimiter, failurePolicy, registry);
    }

    @Override
    public AsyncConditionalWriter withFailurePolicy(FailurePolicy failurePolicy) {
        return new AsyncConditionalWriterImpl(writer, completionExecutor, capacityLimit, capacityLimiter, rateLimiter, failurePolicy, registry);
    }

    @Override
    public SingleWriteStage submit(ConditionalMutation cm) throws InterruptedException {
        long permits = cm.numBytes();
        if (permits > capacityLimit) {
            return failedConditionalWriteFuture(new CapacityExceededException(permits, capacityLimit));
        }
        obeyRateLimit(permits);
        capacityLimiter.acquire(permits);
        return doSubmit(cm, permits);
    }

    @Override
    public SingleWriteStage submitAsync(ConditionalMutation cm, Executor executor) {
        Supplier<SingleWriteStage> submitTask = Interruptible.supplier(() -> submit(cm));
        return asSingleStage(CompletableFuture.supplyAsync(submitTask, executor).thenCompose(Function.identity()));
    }

    @Override
    public SingleWriteStage submit(ConditionalMutation cm, long timeout, TimeUnit unit) throws InterruptedException {
        long permits = cm.numBytes();
        if (permits > capacityLimit) {
            return failedConditionalWriteFuture(new CapacityExceededException(permits, capacityLimit));
        }
        long remaining = obeyRateLimit(permits, timeout, unit);
        if (remaining <= 0) {
            return failedConditionalWriteFuture(SUBMISSION_TIMEOUT);
        }
        boolean acquired = capacityLimiter.tryAcquire(permits, remaining, unit);
        if (!acquired) {
            return failedConditionalWriteFuture(SUBMISSION_TIMEOUT);
        }
        return doSubmit(cm, permits);
    }

    @Override
    public SingleWriteStage submitAsync(ConditionalMutation cm, Executor executor, long timeout, TimeUnit unit) {
        Supplier<SingleWriteStage> submitTask = Interruptible.supplier(() -> submit(cm, timeout, unit));
        return asSingleStage(CompletableFuture.supplyAsync(submitTask, executor).thenCompose(Function.identity()));
    }

    @Override
    public SingleWriteStage trySubmit(ConditionalMutation cm) {
        long permits = cm.numBytes();
        if (permits > capacityLimit) {
            return failedConditionalWriteFuture(new CapacityExceededException(permits, capacityLimit));
        }
        boolean allowed = tryObeyRateLimit(permits);
        if (!allowed) {
            return failedConditionalWriteFuture(SUBMISSION_TIMEOUT);
        }
        boolean acquired = capacityLimiter.tryAcquire(permits);
        if (!acquired) {
            return failedConditionalWriteFuture(SUBMISSION_TIMEOUT);
        }
        return doSubmit(cm, permits);
    }

    private SingleWriteStage doSubmit(ConditionalMutation cm, long permits) {
        Iterator<Result> resultIter = getResultIteratorOrReleasePermits(cm, permits);
        CompleteOneTask task = new CompleteOneTask(resultIter, permits);
        completionExecutor.execute(task);
        barrier.submit(task);
        return task;
    }

    @Override
    public BatchWriteStage submitMany(Collection<ConditionalMutation> mutations) throws InterruptedException {
        long permits = countPermits(mutations);
        if (permits > capacityLimit) {
            return failedConditionalBatchWriteFuture(new CapacityExceededException(permits, capacityLimit));
        }
        obeyRateLimit(permits);
        capacityLimiter.acquire(permits);
        return doSubmitMany(mutations, permits);
    }

    @Override
    public BatchWriteStage submitManyAsync(Collection<ConditionalMutation> mutations, Executor executor) {
        Supplier<BatchWriteStage> submitTask = Interruptible.supplier(() -> submitMany(mutations));
        return asBatchStage(CompletableFuture.supplyAsync(submitTask, executor).thenCompose(Function.identity()));
    }

    @Override
    public BatchWriteStage submitMany(Collection<ConditionalMutation> mutations, long timeout, TimeUnit unit) throws InterruptedException {
        long permits = countPermits(mutations);
        if (permits > capacityLimit) {
            return failedConditionalBatchWriteFuture(new CapacityExceededException(permits, capacityLimit));
        }
        long remaining = obeyRateLimit(permits, timeout, unit);
        if (remaining <= 0) {
            return failedConditionalBatchWriteFuture(SUBMISSION_TIMEOUT);
        }
        capacityLimiter.acquire(permits);
        return doSubmitMany(mutations, permits);
    }

    @Override
    public BatchWriteStage submitManyAsync(Collection<ConditionalMutation> mutations, Executor executor, long timeout, TimeUnit unit) {
        Supplier<BatchWriteStage> submitTask = Interruptible.supplier(() -> submitMany(mutations, timeout, unit));
        return asBatchStage(CompletableFuture.supplyAsync(submitTask, executor).thenCompose(Function.identity()));
    }

    @Override
    public BatchWriteStage trySubmitMany(Collection<ConditionalMutation> mutations) {
        long permits = countPermits(mutations);
        if (permits > capacityLimit) {
            return failedConditionalBatchWriteFuture(new CapacityExceededException(permits, capacityLimit));
        }
        boolean allowed = tryObeyRateLimit(permits);
        if (!allowed) {
            return failedConditionalBatchWriteFuture(SUBMISSION_TIMEOUT);
        }
        boolean acquired = capacityLimiter.tryAcquire(permits);
        if (!acquired) {
            return failedConditionalBatchWriteFuture(SUBMISSION_TIMEOUT);
        }
        return doSubmitMany(mutations, permits);
    }

    private BatchWriteStage doSubmitMany(Collection<ConditionalMutation> mutations, long permits) {
        Iterator<Result> resultIter = getResultIteratorOrReleasePermits(mutations, permits);
        CompleteManyTask task = new CompleteManyTask(resultIter, mutations.size(), permits);
        completionExecutor.execute(task);
        barrier.submit(task);
        return task;
    }

    private void obeyRateLimit(long permits) {
        if (rateLimiter != null) {
            if (permits > Integer.MAX_VALUE) {
                // in practice this should never happen and would
                // probably break Accumulo
                rateLimiter.acquire(Integer.MAX_VALUE);
            } else {
                rateLimiter.acquire((int) permits);
            }
        }
    }

    private long obeyRateLimit(long permits, long timeout, TimeUnit unit) {
        if (rateLimiter != null) {
            long start = System.nanoTime();
            boolean acquired = limitRate(permits, timeout, unit);
            if (!acquired) {
                return 0L;
            }
            return getRemainingInCorrectUnits(timeout, unit, start);
        }
        return timeout;
    }

    private long getRemainingInCorrectUnits(long timeout, TimeUnit unit, long start) {
        long remaining = timeout - (System.nanoTime() - start);
        return unit.convert(remaining, TimeUnit.NANOSECONDS);
    }

    private boolean limitRate(long permits, long timeout, TimeUnit unit) {
        if (permits > Integer.MAX_VALUE) {
            // in practice this should never happen and would probably break Accumulo
            return rateLimiter.tryAcquire(Integer.MAX_VALUE, timeout, unit);
        } else {
            return rateLimiter.tryAcquire((int) permits, timeout, unit);
        }
    }

    private boolean tryObeyRateLimit(long permits) {
        if (rateLimiter != null) {
            if (permits > Integer.MAX_VALUE) {
                // in practice this should never happen and would probably break Accumulo
                return rateLimiter.tryAcquire(Integer.MAX_VALUE);
            }
            return rateLimiter.tryAcquire((int) permits);
        }
        return true;
    }

    private Iterator<Result> getResultIteratorOrReleasePermits(ConditionalMutation cm, long permits) {
        return getResultIteratorOrReleasePermits(Iterators.singletonIterator(cm), permits);
    }

    private Iterator<Result> getResultIteratorOrReleasePermits(Collection<ConditionalMutation> mutations, long permits) {
        return getResultIteratorOrReleasePermits(mutations.iterator(), permits);
    }

    private Iterator<Result> getResultIteratorOrReleasePermits(Iterator<ConditionalMutation> mutations, long permits) {
        try {
            return writer.write(mutations);
        } catch (Throwable e) {
            capacityLimiter.release(permits);
            throw e;
        }
    }

    private long countPermits(Collection<ConditionalMutation> mutations) {
        long permits = 0;
        for (ConditionalMutation mutation : mutations) {
            permits += mutation.numBytes();
        }
        return permits;
    }

    @Override
    public SingleWriteStage asSingleStage(CompletionStage<Result> stage) {
        return asSingleFuture(stage);
    }

    @Override
    public BatchWriteStage asBatchStage(CompletionStage<Collection<Result>> stage) {
        return asBatchFuture(stage);
    }

    @Override
    public <U> WriteStage<U> asWriteStage(CompletionStage<U> stage) {
        return asGenericFuture(stage);
    }

    private SingleWriteFuture asSingleFuture(CompletionStage<Result> stage) {
        CompletableSingleWriteFuture future = new CompletableSingleWriteFuture(this);
        stage.whenComplete(propagateResultTo(future));
        return future;
    }

    private BatchWriteFuture asBatchFuture(CompletionStage<Collection<Result>> stage) {
        CompletableBatchWriteFuture future = new CompletableBatchWriteFuture(this);
        stage.whenComplete(propagateResultTo(future));
        return future;
    }

    private <U> CompletableWriteFuture<U> asGenericFuture(CompletionStage<U> stage) {
        CompletableGenericWriteFuture<U> future = new CompletableGenericWriteFuture<>(this);
        stage.whenComplete(propagateResultTo(future));
        return future;
    }

    @Override
    public void await() throws InterruptedException {
        barrier.await();
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return barrier.await(timeout, unit);
    }

    @Override
    public void close() {
        // the caller is responsible for waiting for writes to complete
        // in which case, shutdownNow will not encounter active or queued tasks.
        completionExecutor.shutdownNow();
        writer.close();
    }

    @Override
    public String toString() {
        return "AsyncConditionalWriterImpl{id=" + id + '}';
    }

    private ExecutorService defaultExecutor() {
        ThreadFactory tf = new ThreadFactoryBuilder()
                .setNameFormat("async-cw-" + id + "-%d")
                // setting daemon in case the application doesn't close resources properly
                .setDaemon(true)
                .build();

        // use an unbounded queue because the semaphore protects against OOME and we want
        // to avoid blocking when submitting tasks for trySubmit methods
        return new ThreadPoolExecutor(0, 16, 60L,
                TimeUnit.SECONDS, new LinkedBlockingQueue<>(), tf, new AbortPolicy());
    }

    /*
     * A task intended to run on a background executor that performs blocking
     * iteration to wait for a conditional write to complete.  Upon completion
     * the task completes this future to notify the submitter and possibly
     * perform dependent completion actions.
     */
    private final class CompleteOneTask extends CompletableSingleWriteFuture implements Runnable, SingleWriteFuture {

        private Iterator<Result> resultIter;
        private final long permits;

        CompleteOneTask(Iterator<Result> resultIter, long permits) {
            super(AsyncConditionalWriterImpl.this);
            this.resultIter = resultIter;
            this.permits = permits;
        }

        public void run() {
            try {
                Result result = getResult();
                if (result == null || failurePolicy.test(result.getStatus())) {
                    complete(result);
                } else {
                    completeExceptionally(new ResultException(result));
                }
            } catch (Throwable e) {
                completeExceptionally(e);
            }
        }

        private Result getResult() {
            try {
                return resultIter.hasNext() ? resultIter.next() : null;
            } finally {
                resultIter = null;
                // release permits prior to calling complete in case the caller
                // schedules dependent completion actions to run in this thread,
                // particularly those that submit more mutations to this writer
                capacityLimiter.release(permits);
            }
        }

    }

    /*
     * A task intended to run on a background executor that performs blocking
     * iteration to wait for a conditional write to complete.  Upon completion
     * the task completes this future to notify the submitter and possibly
     * perform dependent completion actions.
     */
    private final class CompleteManyTask extends CompletableBatchWriteFuture implements Runnable, BatchWriteFuture {

        private Iterator<Result> resultIter;
        private final int count;
        private final long permits;

        CompleteManyTask(Iterator<Result> resultIter, int count, long permits) {
            super(AsyncConditionalWriterImpl.this);
            this.resultIter = resultIter;
            this.count = count;
            this.permits = permits;
        }

        public void run() {
            try {
                Collection<Result> results = getResults();
                if (anyFailed(results)) {
                    completeExceptionally(new ResultBatchException(results));
                } else {
                    complete(results);
                }
            } catch (Throwable e) {
                completeExceptionally(e);
            }
        }

        private boolean anyFailed(Collection<Result> results) throws AccumuloException, AccumuloSecurityException {
            for (Result result : results) {
                if (!failurePolicy.test(result.getStatus())) {
                    return true;
                }
            }
            return false;
        }

        private List<ConditionalWriter.Result> getResults() {
            try {
                return collectResults();
            } finally {
                resultIter = null;
                // release permits prior to calling complete in case the caller
                // schedules dependent completion actions to run in this thread,
                // particularly those that submit more mutations to this writer
                capacityLimiter.release(permits);
            }
        }

        private List<Result> collectResults() {
            List<Result> results = new ArrayList<>(count);
            while (resultIter.hasNext()) {
                results.add(resultIter.next());
            }
            return Collections.unmodifiableList(results);
        }

    }

    interface WriteFuture<T> extends WriteStage<T>, Future<T> {

        @Override
        SingleWriteFuture thenSubmit(ConditionalMutation cm);

        @Override
        SingleWriteFuture thenSubmit(ConditionalMutation cm, long timeout, TimeUnit unit);

        @Override
        SingleWriteFuture thenTrySubmit(ConditionalMutation cm);

        @Override
        BatchWriteFuture thenSubmitMany(Collection<ConditionalMutation> cm);

        @Override
        BatchWriteFuture thenSubmitManyAsync(Collection<ConditionalMutation> cm, Executor executor);

        @Override
        BatchWriteFuture thenSubmitMany(Collection<ConditionalMutation> cm, long timeout, TimeUnit unit);

        @Override
        BatchWriteFuture thenSubmitManyAsync(Collection<ConditionalMutation> cm, Executor executor, long timeout, TimeUnit unit);

        @Override
        BatchWriteFuture thenTrySubmit(Collection<ConditionalMutation> cm);

        @Override
        <U> WriteFuture<U> handle(BiFunction<? super T, Throwable, ? extends U> fn);

        @Override
        <U> WriteFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn);

        @Override
        <U> WriteFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn, Executor executor);

        @Override
        <U> WriteFuture<U> thenApply(Function<? super T, ? extends U> fn);

        @Override
        <U> WriteFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn);

        @Override
        <U> WriteFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn, Executor executor);

        @Override
        <U> WriteFuture<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn);

        @Override
        <U> WriteFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn);

        @Override
        <U> WriteFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn, Executor executor);

        @Override
        WriteFuture<T> whenComplete(BiConsumer<? super T, ? super Throwable> action);

        @Override
        WriteFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action);

        @Override
        WriteFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action, Executor executor);

    }

    interface SingleWriteFuture extends SingleWriteStage, WriteFuture<Result> {}

    interface BatchWriteFuture extends BatchWriteStage, WriteFuture<Collection<Result>> {}

    /*
     * Implements WriteFuture to provide the caller with some syntactic sugar
     * for scheduling dependent writes and other dependent completion actions
     * without giving up that syntactic sugar.
     */
    private static class CompletableWriteFuture<T> extends CompletableFuture<T> implements WriteFuture<T> {

        private final AsyncConditionalWriterImpl writer;

        CompletableWriteFuture(AsyncConditionalWriterImpl writer) {
            this.writer = writer;
        }

        @Override
        public SingleWriteFuture thenSubmit(ConditionalMutation cm) {
            return thenSubmit(result -> writer.submit(cm));
        }

        @Override
        public SingleWriteStage thenSubmitAsync(ConditionalMutation cm, Executor executor) {
            return thenSubmit(result -> writer.submitAsync(cm, executor));
        }

        @Override
        public SingleWriteFuture thenSubmit(ConditionalMutation cm, long timeout, TimeUnit unit) {
            return thenSubmit(result -> writer.submit(cm, timeout, unit));
        }

        @Override
        public SingleWriteStage thenSubmitAsync(ConditionalMutation cm, Executor executor, long timeout, TimeUnit unit) {
            return thenSubmit(result -> writer.submitAsync(cm, executor, timeout, unit));
        }

        @Override
        public SingleWriteFuture thenTrySubmit(ConditionalMutation cm) {
            return thenSubmit(result -> writer.trySubmit(cm));
        }

        private SingleWriteFuture thenSubmit(InterruptibleFunction<T, SingleWriteStage> submitter) {
            return writer.asSingleFuture(super.thenCompose(propagateInterrupt(submitter)));
        }

        @Override
        public BatchWriteFuture thenSubmitMany(Collection<ConditionalMutation> cm) {
            return thenSubmitMany(result -> writer.submitMany(cm));
        }

        @Override
        public BatchWriteFuture thenSubmitManyAsync(Collection<ConditionalMutation> cm, Executor executor) {
            return thenSubmitMany(result -> writer.submitManyAsync(cm, executor));
        }

        @Override
        public BatchWriteFuture thenSubmitMany(Collection<ConditionalMutation> cm, long timeout, TimeUnit unit) {
            return thenSubmitMany(result -> writer.submitMany(cm, timeout, unit));
        }

        @Override
        public BatchWriteFuture thenSubmitManyAsync(Collection<ConditionalMutation> cm, Executor executor, long timeout, TimeUnit unit) {
            return thenSubmitMany(result -> writer.submitManyAsync(cm, executor, timeout, unit));
        }

        @Override
        public BatchWriteFuture thenTrySubmit(Collection<ConditionalMutation> cm) {
            return thenSubmitMany(result -> writer.trySubmitMany(cm));
        }

        private BatchWriteFuture thenSubmitMany(InterruptibleFunction<T, BatchWriteStage> submitter) {
            return writer.asBatchFuture(super.thenCompose(propagateInterrupt(submitter)));
        }

        @Override
        public <U> CompletableWriteFuture<U> thenComposeSubmit(BiFunction<T, AsyncConditionalWriter, CompletionStage<U>> fn) {
            return writer.asGenericFuture(super.thenCompose(result -> fn.apply(result, writer)));
        }

        @Override
        public <U> CompletableWriteFuture<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
            return writer.asGenericFuture(super.handle(fn));
        }

        @Override
        public <U> CompletableWriteFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn) {
            return writer.asGenericFuture(super.handleAsync((fn)));
        }

        @Override
        public <U> CompletableWriteFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn, Executor executor) {
            return writer.asGenericFuture(super.handleAsync(fn, executor));
        }

        @Override
        public <U> CompletableWriteFuture<U> thenApply(Function<? super T, ? extends U> fn) {
            return writer.asGenericFuture(super.thenApply(fn));
        }

        @Override
        public <U> CompletableWriteFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn) {
            return writer.asGenericFuture(super.thenApplyAsync(fn));
        }

        @Override
        public <U> CompletableWriteFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn, Executor executor) {
            return writer.asGenericFuture(super.thenApplyAsync(fn, executor));
        }

        @Override
        public <U> CompletableWriteFuture<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn) {
            return writer.asGenericFuture(super.thenCompose(fn));
        }

        @Override
        public <U> CompletableWriteFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn) {
            return writer.asGenericFuture(super.thenComposeAsync(fn));
        }

        @Override
        public <U> CompletableWriteFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn, Executor executor) {
            return writer.asGenericFuture(super.thenComposeAsync(fn, executor));
        }

        @Override
        public CompletableWriteFuture<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
            return writer.asGenericFuture(super.whenComplete(action));
        }

        @Override
        public CompletableWriteFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action) {
            return writer.asGenericFuture(super.whenCompleteAsync(action));
        }

        @Override
        public CompletableWriteFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action, Executor executor) {
            return writer.asGenericFuture(super.whenCompleteAsync(action, executor));
        }

    }

    private static class CompletableSingleWriteFuture extends CompletableWriteFuture<Result> implements SingleWriteFuture {
        CompletableSingleWriteFuture(AsyncConditionalWriterImpl writer) {
            super(writer);
        }
    }

    private static class CompletableBatchWriteFuture extends CompletableWriteFuture<Collection<Result>> implements BatchWriteFuture {
        CompletableBatchWriteFuture(AsyncConditionalWriterImpl writer) {
            super(writer);
        }
    }

    private static class CompletableGenericWriteFuture<T> extends CompletableWriteFuture<T> implements WriteFuture<T> {
        CompletableGenericWriteFuture(AsyncConditionalWriterImpl writer) {
            super(writer);
        }
    }

    private SingleWriteFuture failedConditionalWriteFuture(Throwable t) {
        CompletableSingleWriteFuture f = new CompletableSingleWriteFuture(this);
        f.completeExceptionally(t);
        return f;
    }

    private BatchWriteFuture failedConditionalBatchWriteFuture(Throwable t) {
        CompletableBatchWriteFuture f = new CompletableBatchWriteFuture(this);
        f.completeExceptionally(t);
        return f;
    }

}
