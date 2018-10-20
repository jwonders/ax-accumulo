package com.jwsphere.accumulo.async.internal;

import com.google.common.util.concurrent.RateLimiter;
import com.jwsphere.accumulo.async.AsyncBatchWriter;
import com.jwsphere.accumulo.async.AsyncMultiTableBatchWriter;
import com.jwsphere.accumulo.async.AsyncMultiTableBatchWriterConfig;
import com.jwsphere.accumulo.async.SubmissionTimeoutException;
import com.jwsphere.accumulo.async.internal.Interruptible.InterruptibleFunction;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.jwsphere.accumulo.async.internal.Interruptible.propagateInterrupt;
import static com.jwsphere.accumulo.async.internal.MoreCompletableFutures.propagateResultTo;

/**
 * The asynchronous batch writer allows a bounded set of mutations to be
 * in the process of being written to Accumulo and informs the submitter
 * upon completion or error.
 *
 * @author Jonathan Wonders
 */
public class AsyncMultiTableBatchWriterImpl implements AsyncMultiTableBatchWriter {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncMultiTableBatchWriterImpl.class);

    private static final SubmissionTimeoutException SUBMISSION_TIMEOUT = new SubmissionTimeoutException();

    private static final AtomicLong WRITER_ID = new AtomicLong(0);

    /**
     * Upon encountering a {@code MutationsRejectedException}, it is recommended to
     * close the batch writer and create a new one.  This factory allows the async
     * writer to recreate writers as necessary. See ACCUMULO-2990 for details.
     */
    private final WriterFactory writerFactory;

    /**
     * The current multi-table batch writer used to write mutations.  Upon encountering
     * a {@code MutationsRejectedException} the writer will be recreated.
     */
    private MultiTableBatchWriter writer;

    private long id = WRITER_ID.getAndIncrement();

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final FlushTask flushTask;
    private final LongSemaphore capacityLimit = new LongSemaphore(50 * 1024 * 1024);
    private final AsyncMultiTableBatchWriter.ListenerFactory listenerFactory;

    /**
     * Creates an async multi table batch writer.  Ownership of the writer
     * is transferred to this instance in order to ensure it is properly closed.
     *
     * @param writerFactory A factory that can create writers.  If a writer fails, a new one will
     *                      be requested to ensure the failure did not affect its internal state.
     */
    public AsyncMultiTableBatchWriterImpl(WriterFactory writerFactory) {
        this(writerFactory, AsyncMultiTableBatchWriterConfig.create(new BatchWriterConfig()));
    }

    public AsyncMultiTableBatchWriterImpl(WriterFactory writerFactory, AsyncMultiTableBatchWriterConfig config) {
        this.writerFactory = writerFactory;
        this.writer = writerFactory.create();
        this.listenerFactory = config.getListenerFactory();
        this.flushTask = new FlushTask(config.getMaxBytesPerFlush(), config.getMaxFlushRatePerSecond());
        this.executorService.submit(flushTask);
    }

    @Override
    public WriteStage submit(String table, Mutation mutation) throws InterruptedException {
        return flushTask.submit(createMutation(table, mutation));
    }

    @Override
    public WriteStage submitAsync(String table, Mutation mutation, Executor executor) {
        Supplier<WriteStage> submitTask = Interruptible.supplier(() -> submit(table, mutation));
        return asWriteFuture(CompletableFuture.supplyAsync(submitTask, executor).thenCompose(Function.identity()));
    }

    @Override
    public WriteStage submit(String table, Mutation mutation, long timeout, TimeUnit unit) throws InterruptedException {
        return flushTask.submit(createMutation(table, mutation), timeout, unit);
    }

    @Override
    public WriteStage submitAsync(String table, Mutation mutation, Executor executor, long timeout, TimeUnit unit) {
        Supplier<WriteStage> submitTask = Interruptible.supplier(() -> submit(table, mutation, timeout, unit));
        return asWriteFuture(CompletableFuture.supplyAsync(submitTask, executor).thenCompose(Function.identity()));
    }

    @Override
    public WriteStage trySubmit(String table, Mutation mutation) {
        return flushTask.trySubmit(createMutation(table, mutation));
    }

    @Override
    public WriteStage submitMany(String table, Collection<Mutation> mutations) throws InterruptedException {
        return flushTask.submit(createMutation(table, mutations));
    }

    @Override
    public WriteStage submitManyAsync(String table, Collection<Mutation> mutations, Executor executor) {
        Supplier<WriteStage> submitTask = Interruptible.supplier(() -> submitMany(table, mutations));
        return asWriteFuture(CompletableFuture.supplyAsync(submitTask, executor).thenCompose(Function.identity()));
    }

    @Override
    public WriteStage submitMany(String table, Collection<Mutation> mutations, long timeout, TimeUnit unit) throws InterruptedException {
        return flushTask.submit(createMutation(table, mutations), timeout, unit);
    }

    @Override
    public WriteStage submitManyAsync(String table, Collection<Mutation> mutations, Executor executor, long timeout, TimeUnit unit) {
        Supplier<WriteStage> submitTask = Interruptible.supplier(() -> submitMany(table, mutations, timeout, unit));
        return asWriteFuture(CompletableFuture.supplyAsync(submitTask, executor).thenCompose(Function.identity()));
    }

    @Override
    public WriteStage trySubmitMany(String table, Collection<Mutation> mutations) {
        return flushTask.trySubmit(createMutation(table, mutations));
    }

    @Override
    public AsyncBatchWriter getBatchWriter(String table) {
        return new AsyncBatchWriterImpl(table);
    }

    @Override
    public void await() throws InterruptedException {
        flushTask.submit(new Await(this)).join();
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        try {
            flushTask.submit(new Await(this)).get(timeout, unit);
            return true;
        } catch (ExecutionException e) {
            throw new CompletionException(e);
        } catch (TimeoutException e) {
            return false;
            // normal behavior
        }
    }

    @Override
    public void close() {
        // request that the flush task stops after any active flush completes
        flushTask.shutdown();

        if (writer != null) {
            try {
                writer.close();
            } catch (MutationsRejectedException e) {
                throw new RuntimeException(e);
            }
        }

        // assuming the callers have stopped submitting tasks and there
        // is not an issue preventing the completion of tasks, interruption
        // is not necessary to achieve an orderly shutdown.
        // interruption is honored as a fallback.

        executorService.shutdownNow();
    }

    private CompletableWriteFuture asWriteFuture(CompletionStage<Void> stage) {
        CompletableWriteFuture future = new CompletableWriteFuture(this);
        stage.whenComplete(propagateResultTo(future));
        return future;
    }

    private FutureMutation createMutation(String table, Mutation mutation) {
        return new FutureSingleMutation(this, table, mutation, capacityLimit);
    }

    private FutureMutation createMutation(String table, Collection<Mutation> mutation) {
        return new FutureMutationBatch(this, table, mutation, capacityLimit);
    }

    class AsyncBatchWriterImpl implements AsyncBatchWriter {

        private final WriterStrategy strategy;
        private final CompletionBarrier barrier;

        AsyncBatchWriterImpl(String table) {
            this.strategy = new SingleTableWriterStrategy(table);
            this.barrier = new CompletionBarrier();
        }

        @Override
        public WriteStage submit(Mutation mutation) throws InterruptedException {
            return propagateCompletionThenTrack(flushTask.submit(createMutation(mutation)));
        }

        @Override
        public WriteStage submitAsync(Mutation mutation, Executor executor) {
            Supplier<WriteStage> submitTask = Interruptible.supplier(() -> submit(mutation));
            return asWriteFuture(CompletableFuture.supplyAsync(submitTask, executor).thenCompose(Function.identity()));
        }

        @Override
        public WriteStage submit(Mutation mutation, long timeout, TimeUnit unit) throws InterruptedException {
            return propagateCompletionThenTrack(flushTask.submit(createMutation(mutation), timeout, unit));
        }

        @Override
        public WriteStage submitAsync(Mutation mutation, Executor executor, long timeout, TimeUnit unit) {
            Supplier<WriteStage> submitTask = Interruptible.supplier(() -> submit(mutation, timeout, unit));
            return asWriteFuture(CompletableFuture.supplyAsync(submitTask, executor).thenCompose(Function.identity()));
        }

        @Override
        public WriteStage trySubmit(Mutation mutation) {
            return propagateCompletionThenTrack(flushTask.trySubmit(createMutation(mutation)));
        }

        @Override
        public WriteStage submitMany(Collection<Mutation> mutations) throws InterruptedException {
            return propagateCompletionThenTrack(flushTask.submit(createMutation(mutations)));
        }

        @Override
        public WriteStage submitManyAsync(Collection<Mutation> mutations, Executor executor) {
            Supplier<WriteStage> submitTask = Interruptible.supplier(() -> submitMany(mutations));
            return asWriteFuture(CompletableFuture.supplyAsync(submitTask, executor).thenCompose(Function.identity()));
        }

        @Override
        public WriteStage submitMany(Collection<Mutation> mutations, long timeout, TimeUnit unit) throws InterruptedException {
            return propagateCompletionThenTrack(flushTask.submit(createMutation(mutations), timeout, unit));
        }

        @Override
        public WriteStage submitManyAsync(Collection<Mutation> mutations, Executor executor, long timeout, TimeUnit unit) {
            Supplier<WriteStage> submitTask = Interruptible.supplier(() -> submitMany(mutations, timeout, unit));
            return asWriteFuture(CompletableFuture.supplyAsync(submitTask, executor).thenCompose(Function.identity()));
        }

        @Override
        public WriteStage trySubmitMany(Collection<Mutation> mutations) {
            return propagateCompletionThenTrack(flushTask.trySubmit(createMutation(mutations)));
        }

        @Override
        public WriteStage asWriteStage(CompletionStage<Void> stage) {
            return asWriteFuture(stage);
        }

        private TableWriteFuture asWriteFuture(CompletionStage<Void> stage) {
            CompletableTableWriteFuture future = new CompletableTableWriteFuture(this);
            stage.whenComplete(propagateResultTo(future));
            return future;
        }

        private FutureSingleMutation createMutation(Mutation mutation) {
            AsyncMultiTableBatchWriterImpl writer = AsyncMultiTableBatchWriterImpl.this;
            return new FutureSingleMutation(writer, strategy, mutation, capacityLimit);
        }

        private FutureMutationBatch createMutation(Collection<Mutation> mutations) {
            AsyncMultiTableBatchWriterImpl writer = AsyncMultiTableBatchWriterImpl.this;
            return new FutureMutationBatch(writer, strategy, mutations, capacityLimit);
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
            throw new UnsupportedOperationException("Close the underlying AsyncMultiTableBatchWriter instead.");
        }

        private WriteStage propagateCompletionThenTrack(AsyncMultiTableBatchWriter.WriteStage stage) {
            WriteStage wrapped = asWriteFuture(stage);
            barrier.submit(wrapped);
            return wrapped;
        }

    }

    private static abstract class FutureMutation extends CompletableWriteFuture {

        private final long created;

        FutureMutation(AsyncMultiTableBatchWriterImpl writer) {
            super(writer);
            this.created = System.currentTimeMillis();
        }

        abstract long size();

        abstract void acquire() throws InterruptedException;

        abstract boolean acquire(long timeout, TimeUnit unit) throws InterruptedException;

        abstract boolean tryAcquire();

        abstract void submit(MultiTableBatchWriter writer) throws MutationsRejectedException;

        /*
         * Not overriding completeExceptionally because that would either allow the
         * submitter to call a method that releases permits or that may encounter a
         * race condition.  These complete methods should ONLY be called following
         * the acquisition of permits.
         */
        abstract boolean internalCompleteExceptionally(Throwable t);

        /*
         * Not overriding completeExceptionally because that would either allow the
         * submitter to call a method that releases permits or that may encounter a
         * race condition.
         */
        abstract boolean internalComplete();

        public long msSinceSubmissionRequest() {
            return System.currentTimeMillis() - created;
        }

    }

    private static abstract class CapacityLimitedFutureMutation extends FutureMutation {

        private final long permits;
        private LongSemaphore capacityLimit;

        CapacityLimitedFutureMutation(AsyncMultiTableBatchWriterImpl writer, long permits, LongSemaphore capacityLimit) {
            super(writer);
            this.permits = permits;
            this.capacityLimit = capacityLimit;
        }

        @Override
        public long size() {
            return permits;
        }

        @Override
        void acquire() throws InterruptedException {
            capacityLimit.acquire(permits);
        }

        @Override
        boolean acquire(long timeout, TimeUnit unit) throws InterruptedException {
            return capacityLimit.tryAcquire(permits, timeout, unit);
        }

        boolean tryAcquire() {
            return capacityLimit.tryAcquire(permits);
        }

        @Override
        boolean internalCompleteExceptionally(Throwable t) {
            if (capacityLimit != null) {
                capacityLimit.release(permits);
                capacityLimit = null;
                return completeExceptionally(t);
            }
            throw new IllegalStateException("Only one complete method may be called exactly once.");
        }

        @Override
        boolean internalComplete() {
            if (capacityLimit != null) {
                capacityLimit.release(permits);
                capacityLimit = null;
                return complete(null);
            }
            throw new IllegalStateException("Only one complete method may be called exactly once.");
        }

    }

    interface WriterStrategy {

        BatchWriter getWriter(MultiTableBatchWriter writer) throws AccumuloException, AccumuloSecurityException, TableNotFoundException;

    }

    /*
     * If the table name is known up front as with an AsyncBatchWriter the writer
     * can be cached as long as the multi-table writer is not invalidated.  Since
     * all flushes are performed sequentially on a single thread, synchronization
     * is unnecessary.
     */
    private static final class SingleTableWriterStrategy implements WriterStrategy {

        private final String table;

        private MultiTableBatchWriter multiTableBatchWriter;
        private BatchWriter writer;

        public SingleTableWriterStrategy(String table) {
            this.table = table;
        }

        public BatchWriter getWriter(MultiTableBatchWriter writer) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
            if (this.writer == null || this.multiTableBatchWriter != writer) {
                this.multiTableBatchWriter = writer;
                this.writer = multiTableBatchWriter.getBatchWriter(table);
            }
            return this.writer;
        }

    }

    private static WriterStrategy multiTableWriterStrategy(String table) {
        return writer -> writer.getBatchWriter(table);
    }

    private static final class FutureSingleMutation extends CapacityLimitedFutureMutation {

        private final WriterStrategy strategy;
        private final Mutation mutation;

        FutureSingleMutation(AsyncMultiTableBatchWriterImpl writer, String table, Mutation mutation, LongSemaphore capacityLimit) {
            this(writer, multiTableWriterStrategy(table), mutation, capacityLimit);
        }

        FutureSingleMutation(AsyncMultiTableBatchWriterImpl writer, WriterStrategy strategy, Mutation mutation, LongSemaphore capacityLimit) {
            super(writer, mutation.estimatedMemoryUsed(), capacityLimit);
            this.strategy = strategy;
            this.mutation = mutation;
        }

        @Override
        public void submit(MultiTableBatchWriter writer) {
            try {
                strategy.getWriter(writer).addMutation(mutation);
            } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException | RuntimeException e) {
                completeExceptionally(e);
            }
        }

    }

    private static final class FutureMutationBatch extends CapacityLimitedFutureMutation {

        private final WriterStrategy strategy;
        private final Collection<Mutation> mutations;

        FutureMutationBatch(AsyncMultiTableBatchWriterImpl writer, String table, Collection<Mutation> mutations, LongSemaphore capacityLimit) {
            this(writer, multiTableWriterStrategy(table), mutations, capacityLimit);
        }

        FutureMutationBatch(AsyncMultiTableBatchWriterImpl writer, WriterStrategy strategy, Collection<Mutation> mutations, LongSemaphore capacityLimit) {
            super(writer, computePermits(mutations), capacityLimit);
            this.strategy = strategy;
            this.mutations = mutations;
        }

        @Override
        public void submit(MultiTableBatchWriter writer) {
            try {
                strategy.getWriter(writer).addMutations(mutations);
            } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException | RuntimeException e) {
                completeExceptionally(e);
            }
        }

        private static long computePermits(Collection<Mutation> mutations) {
            long permits = 0;
            for (Mutation mutation : mutations) {
                permits += mutation.estimatedMemoryUsed();
            }
            return permits;
        }

    }

    private static final class Await extends FutureMutation {

        Await(AsyncMultiTableBatchWriterImpl writer) {
            super(writer);
        }

        @Override
        long size() {
            return 0L;
        }

        @Override
        void acquire() {
        }

        @Override
        boolean acquire(long timeout, TimeUnit unit) {
            return true;
        }

        @Override
        boolean tryAcquire() {
            return true;
        }

        @Override
        void submit(MultiTableBatchWriter writer) {
            // nothing to submit
        }

        @Override
        boolean internalCompleteExceptionally(Throwable t) {
            return completeExceptionally(t);
        }

        @Override
        boolean internalComplete() {
            return complete(null);
        }

    }

    private final class FlushTask implements Runnable {

        private final BlockingQueue<FutureMutation> queue;
        private final List<FutureMutation> batch;
        private final RateLimiter limiter;
        private final long batchCapacityInBytes;

        private volatile boolean shutdown = false;

        private AsyncMultiTableBatchWriter.Listener metrics;

        FlushTask(long batchCapacity, double flushesPerSecondLimit) {
            this.batchCapacityInBytes = batchCapacity;
            this.queue = new LinkedBlockingQueue<>();
            this.batch = new ArrayList<>();
            this.limiter = RateLimiter.create(flushesPerSecondLimit);
            this.metrics = listenerFactory.create(id);
        }

        FutureMutation submit(FutureMutation mutation) throws InterruptedException {
            ensureNotShutdown();
            mutation.acquire();
            return addToQueue(mutation);
        }

        FutureMutation submit(FutureMutation mutation, long timeout, TimeUnit unit) throws InterruptedException {
            ensureNotShutdown();
            boolean acquired = mutation.acquire(timeout, unit);
            if (acquired) {
                return addToQueue(mutation);
            }
            mutation.completeExceptionally(SUBMISSION_TIMEOUT);
            return mutation;
        }

        FutureMutation trySubmit(FutureMutation mutation) {
            ensureNotShutdown();
            boolean acquired = mutation.tryAcquire();
            if (acquired) {
                return addToQueue(mutation);
            }
            mutation.completeExceptionally(SUBMISSION_TIMEOUT);
            return mutation;
        }

        private FutureMutation addToQueue(FutureMutation mutation) {
            boolean submitted = queue.offer(mutation);
            if (!submitted) {
                // submission to an unbounded queue should not fail
                mutation.completeExceptionally(new RuntimeException());
                return mutation;
            }
            metrics.recordWaitTime(id, mutation.msSinceSubmissionRequest(), TimeUnit.MILLISECONDS);
            metrics.recordSubmission(id);
            return mutation;
        }

        private void ensureNotShutdown() {
            if (shutdown) {
                throw new IllegalStateException("Cannot submit mutations after a shutdown.");
            }
        }

        @Override
        public void run() {
            try {
                while (!Thread.currentThread().isInterrupted() && !shutdown) {
                    try {
                        nextBatch();
                    } catch (InterruptedException e) {
                        // inform the executor about the interrupt
                        Thread.currentThread().interrupt();
                        return;
                    }
                    try {
                        FlushEvent event = metrics.startFlushEvent();
                        event.recordBatchSize(batch.size());
                        event.recordRemainingQueueDepth(queue.size());

                        long batchBytes = submitCurrentBatch();
                        event.recordBatchEstimatedMemory(batchBytes);

                        // control the rate of RPCs to avoid many small requests
                        event.beforeRateLimiting();
                        limiter.acquire();
                        event.afterRateLimiting();

                        for (FutureMutation fm : batch) {
                            metrics.recordWaitTime(id, fm.msSinceSubmissionRequest(), TimeUnit.MILLISECONDS);
                        }

                        event.beforeFlush();
                        writer.flush();
                        event.afterFlush();

                        completeAll(batch);
                    } catch (MutationsRejectedException e) {
                        failAll(batch, e);
                        LOG.debug("Recreating writer due to rejected mutations", e);
                        try {
                            writer.close();
                        } catch (MutationsRejectedException x) {
                            // suppress because we already failed everything
                        }
                        writer = writerFactory.create();
                    } catch (RuntimeException e) {
                        handleRuntimeException(batch, e);
                    } finally {
                        for (FutureMutation fm : batch) {
                            metrics.recordWriteLatency(id, fm.msSinceSubmissionRequest(), TimeUnit.MILLISECONDS);
                        }
                        batch.clear();
                    }
                }
            } finally {
                try {
                    writer.close();
                } catch (MutationsRejectedException e) {
                    // everything must have already been failed
                }
            }
        }

        private void nextBatch() throws InterruptedException {
            long batchSize = 0;
            FutureMutation first = queue.take();
            batchSize += first.size();
            batch.add(first);

            FutureMutation fm;
            while ((fm = queue.peek()) != null) {
                if (batchSize + fm.size() < batchCapacityInBytes) {
                    batchSize += fm.size();
                    batch.add(queue.poll());
                } else {
                    break;
                }
            }
        }

        private long submitCurrentBatch() throws MutationsRejectedException {
            long batchBytes = 0;
            for (FutureMutation mutation : batch) {
                batchBytes += mutation.size();
                mutation.submit(writer);
            }
            return batchBytes;
        }

        private void handleRuntimeException(List<FutureMutation> batch, RuntimeException e) {
            // Accumulo wraps InterruptedException and does not set the thread's interrupt flag
            // we may see this during a call to AsyncMultiTableBatchWriter::shutdownNow()
            if (e.getCause() instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                failAll(batch, e.getCause());
            } else {
                // There is some other unexpected error.  We could try to do something like
                // recreate the multi table batch writer.  Or we could propagate it to the
                // owning async multi table batch writer.
                failAll(batch, e);
                throw e;
            }
        }

        private void completeAll(List<FutureMutation> batch) {
            for (FutureMutation mutation : batch) {
                mutation.internalComplete();
            }
        }

        private void failAll(List<FutureMutation> batch, Throwable cause) {
            for (FutureMutation mutation : batch) {
                mutation.internalCompleteExceptionally(cause);
            }
        }

        void shutdown() {
            this.shutdown = true;
        }

    }

    public interface WriteFuture extends Future<Void>, WriteStage {

        WriteFuture thenSubmit(String table, Mutation mutation);

        WriteFuture thenSubmitAsync(String table, Mutation mutation, Executor executor);

        WriteFuture thenSubmit(String table, Mutation mutation, long timeout, TimeUnit unit);

        WriteFuture thenSubmitAsync(String table, Mutation mutation, Executor executor, long timeout, TimeUnit unit);

        WriteFuture thenTrySubmit(String table, Mutation mutation);

        WriteFuture thenSubmitMany(String table, Collection<Mutation> mutations);

        WriteFuture thenSubmitManyAsync(String table, Collection<Mutation> mutations, Executor executor);

        WriteFuture thenSubmitMany(String table, Collection<Mutation> mutations, long timeout, TimeUnit unit);

        WriteFuture thenSubmitManyAsync(String table, Collection<Mutation> mutations, Executor executor, long timeout, TimeUnit unit);

        WriteFuture thenTrySubmitMany(String table, Collection<Mutation> mutations);

    }

    private static class CompletableWriteFuture extends CompletableFuture<Void> implements WriteFuture {

        private final AsyncMultiTableBatchWriterImpl writer;

        CompletableWriteFuture(AsyncMultiTableBatchWriterImpl writer) {
            this.writer = writer;
        }

        @Override
        public WriteFuture thenSubmit(String table, Mutation mutation) {
            return thenSubmit(result -> writer.submit(table, mutation));
        }

        @Override
        public WriteFuture thenSubmitAsync(String table, Mutation mutation, Executor executor) {
            return thenSubmit(result -> writer.submitAsync(table, mutation, executor));
        }

        @Override
        public WriteFuture thenSubmit(String table, Mutation mutation, long timeout, TimeUnit unit) {
            return thenSubmit(result -> writer.submit(table, mutation, timeout, unit));
        }

        @Override
        public WriteFuture thenSubmitAsync(String table, Mutation mutation, Executor executor, long timeout, TimeUnit unit) {
            return thenSubmit(result -> writer.submitAsync(table, mutation, executor, timeout, unit));
        }

        @Override
        public WriteFuture thenTrySubmit(String table, Mutation mutation) {
            return thenSubmit(result -> writer.trySubmit(table, mutation));
        }

        @Override
        public WriteFuture thenSubmitMany(String table, Collection<Mutation> mutations) {
            return thenSubmit(result -> writer.submitMany(table, mutations));
        }

        @Override
        public WriteFuture thenSubmitManyAsync(String table, Collection<Mutation> mutations, Executor executor) {
            return thenSubmit(result -> writer.submitManyAsync(table, mutations, executor));
        }

        @Override
        public WriteFuture thenSubmitMany(String table, Collection<Mutation> mutations, long timeout, TimeUnit unit) {
            return thenSubmit(result -> writer.submitMany(table, mutations, timeout, unit));
        }

        @Override
        public WriteFuture thenSubmitManyAsync(String table, Collection<Mutation> mutations, Executor executor, long timeout, TimeUnit unit) {
            return thenSubmit(result -> writer.submitManyAsync(table, mutations, executor, timeout, unit));
        }

        @Override
        public WriteFuture thenTrySubmitMany(String table, Collection<Mutation> mutations) {
            return thenSubmit(result -> writer.trySubmitMany(table, mutations));
        }

        private WriteFuture thenSubmit(InterruptibleFunction<Void, WriteStage> submitter) {
            return writer.asWriteFuture(super.thenCompose(propagateInterrupt(submitter)));
        }

    }

    public interface TableWriteFuture extends Future<Void>, AsyncBatchWriter.WriteStage {

        TableWriteFuture thenSubmit(Mutation mutation);

        TableWriteFuture thenSubmitAsync(Mutation mutation, Executor executor);

        TableWriteFuture thenSubmit(Mutation mutation, long timeout, TimeUnit unit);

        TableWriteFuture thenSubmitAsync(Mutation mutation, Executor executor, long timeout, TimeUnit unit);

        TableWriteFuture thenTrySubmit(Mutation mutation);

        TableWriteFuture thenSubmitMany(Collection<Mutation> mutations);

        TableWriteFuture thenSubmitManyAsync(Collection<Mutation> mutations, Executor executor);

        TableWriteFuture thenSubmitMany(Collection<Mutation> mutations, long timeout, TimeUnit unit);

        TableWriteFuture thenSubmitManyAsync(Collection<Mutation> mutations, Executor executor, long timeout, TimeUnit unit);

        TableWriteFuture thenTrySubmitMany(Collection<Mutation> mutations);

    }

    private static class CompletableTableWriteFuture extends CompletableFuture<Void> implements TableWriteFuture {

        private final AsyncBatchWriterImpl writer;

        CompletableTableWriteFuture(AsyncBatchWriterImpl writer) {
            this.writer = writer;
        }

        @Override
        public TableWriteFuture thenSubmit(Mutation mutation) {
            return thenSubmit(result -> writer.submit(mutation));
        }

        @Override
        public TableWriteFuture thenSubmitAsync(Mutation mutation, Executor executor) {
            return thenSubmit(result -> writer.submitAsync(mutation, executor));
        }

        @Override
        public TableWriteFuture thenSubmit(Mutation mutation, long timeout, TimeUnit unit) {
            return thenSubmit(result -> writer.submit(mutation, timeout, unit));
        }

        @Override
        public TableWriteFuture thenSubmitAsync(Mutation mutation, Executor executor, long timeout, TimeUnit unit) {
            return thenSubmit(result -> writer.submitAsync(mutation, executor, timeout, unit));
        }

        @Override
        public TableWriteFuture thenTrySubmit(Mutation mutation) {
            return thenSubmit(result -> writer.trySubmit(mutation));
        }

        @Override
        public TableWriteFuture thenSubmitMany(Collection<Mutation> mutations) {
            return thenSubmit(result -> writer.submitMany(mutations));
        }

        @Override
        public TableWriteFuture thenSubmitManyAsync(Collection<Mutation> mutations, Executor executor) {
            return thenSubmit(result -> writer.submitManyAsync(mutations, executor));
        }

        @Override
        public TableWriteFuture thenSubmitMany(Collection<Mutation> mutations, long timeout, TimeUnit unit) {
            return thenSubmit(result -> writer.submitMany(mutations, timeout, unit));
        }

        @Override
        public TableWriteFuture thenSubmitManyAsync(Collection<Mutation> mutations, Executor executor, long timeout, TimeUnit unit) {
            return thenSubmit(result -> writer.submitManyAsync(mutations, executor, timeout, unit));
        }

        @Override
        public TableWriteFuture thenTrySubmitMany(Collection<Mutation> mutations) {
            return thenSubmit(result -> writer.trySubmitMany(mutations));
        }

        private TableWriteFuture thenSubmit(InterruptibleFunction<Void, AsyncBatchWriter.WriteStage> submitter) {
            return writer.asWriteFuture(super.thenCompose(propagateInterrupt(submitter)));
        }

    }

    @FunctionalInterface
    public interface WriterFactory {

        MultiTableBatchWriter create();

    }

}
