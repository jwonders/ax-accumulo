package com.jwsphere.accumulo.async.internal;

import com.google.common.util.concurrent.RateLimiter;
import com.jwsphere.accumulo.async.AsyncBatchWriter;
import com.jwsphere.accumulo.async.AsyncBatchWriter.WriteStage;
import com.jwsphere.accumulo.async.AsyncMultiTableBatchWriter;
import com.jwsphere.accumulo.async.MutationWriteStage;
import com.jwsphere.accumulo.async.SubmissionTimeoutException;
import org.HdrHistogram.Histogram;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static com.jwsphere.accumulo.async.internal.Interruptible.propagateInterrupt;

/**
 * @author Jonathan Wonders
 */
public class AsyncMultiTableBatchWriterImpl implements AsyncMultiTableBatchWriter {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncMultiTableBatchWriterImpl.class);
    private static final SubmissionTimeoutException SUBMISSION_TIMEOUT = new SubmissionTimeoutException();

    private final Supplier<MultiTableBatchWriter> writerSupplier;

    /**
     * The current multi-table batch writer used to write mutations.  Upon encountering
     * a {@code MutationsRejectedException} the writer should be recreated.
     */
    private MultiTableBatchWriter writer;

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final FlushTask flushTask;
    private final LongSemaphore capacityLimit = new LongSemaphore(50 * 1024 * 1024);

    private final Histogram submitTimes = new Histogram(5);
    private final Histogram flushTimes = new Histogram(3);

    /**
     * Creates an async multi table batch writer.  Ownership of the writer
     * is transferred to this instance in order to ensure it is properly closed.
     *
     * @param writerSupplier The writer to submit mutations to.
     */
    public AsyncMultiTableBatchWriterImpl(Supplier<MultiTableBatchWriter> writerSupplier) {
        this.writerSupplier = writerSupplier;
        this.writer = writerSupplier.get();
        this.flushTask = new FlushTask(4 * 1024 * 1024, 100);
        this.executorService.submit(flushTask);
    }

    @Override
    public MutationWriteStage submit(String table, Mutation mutation) throws InterruptedException {
        return flushTask.submit(createMutation(table, mutation));
    }

    @Override
    public MutationWriteStage submitAsync(String table, Mutation mutation, Executor executor) {
        return null;
    }

    @Override
    public MutationWriteStage submit(String table, Mutation mutation, long timeout, TimeUnit unit) throws InterruptedException {
        return flushTask.submit(createMutation(table, mutation), timeout, unit);
    }

    @Override
    public MutationWriteStage submitAsync(String table, Mutation mutation, Executor executor, long timeout, TimeUnit unit) {
        return null;
    }

    @Override
    public MutationWriteStage trySubmit(String table, Mutation mutation) {
        return flushTask.trySubmit(createMutation(table, mutation));
    }

    @Override
    public MutationWriteStage submitMany(String table, Collection<Mutation> mutations) throws InterruptedException {
        return flushTask.submit(createMutation(table, mutations));
    }

    @Override
    public MutationWriteStage submitManyAsync(String table, Collection<Mutation> mutations, Executor executor) {
        return null;
    }

    @Override
    public MutationWriteStage submitMany(String table, Collection<Mutation> mutations, long timeout, TimeUnit unit) throws InterruptedException {
        return flushTask.submit(createMutation(table, mutations), timeout, unit);
    }

    @Override
    public MutationWriteStage submitManyAsync(String table, Collection<Mutation> mutations, Executor executor, long timeout, TimeUnit unit) {
        return null;
    }

    @Override
    public MutationWriteStage trySubmitMany(String table, Collection<Mutation> mutations) {
        return flushTask.trySubmit(createMutation(table, mutations));
    }

    @Override
    public AsyncBatchWriter getBatchWriter(String table) {
        try {
            return new AsyncBatchWriterImpl(table);
        } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void await() throws InterruptedException {
        flushTask.submit(new Await()).join();
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        try {
            flushTask.submit(new Await()).get(timeout, unit);
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

        // assuming the callers have stopped submitting tasks and there
        // is not an issue preventing the completion of tasks, interruption
        // is not necessary to achieve an orderly shutdown.
        // interruption is honored as a fallback.

        executorService.shutdownNow();
    }

    private FutureMutation createMutation(String table, Mutation mutation) {
        return new FutureSingleMutation(this, table, mutation, capacityLimit);
    }

    private FutureMutation createMutation(String table, Collection<Mutation> mutation) {
        return new FutureMutationBatch(this, table, mutation, capacityLimit);
    }

    class TableSpecificStage extends CompletableFuture<Void> implements AsyncBatchWriter.WriteStage {

        private final String table;
        private final MutationWriteStage stage;

        TableSpecificStage(String table, MutationWriteStage stage) {
            this.table = table;
            this.stage = stage;

        }

        @Override
        public WriteStage thenSubmit(Mutation mutation) {
            return propagateCompletion(stage.thenSubmit(table, mutation));
        }

        @Override
        public WriteStage thenSubmit(Mutation mutation, long timeout, TimeUnit unit) {
            return propagateCompletion(stage.thenSubmit(table, mutation, timeout, unit));
        }

        @Override
        public WriteStage thenTrySubmit(Mutation mutation) {
            return propagateCompletion(stage.thenTrySubmit(table, mutation));
        }

        @Override
        public WriteStage thenSubmit(Collection<Mutation> mutations) {
            return propagateCompletion(stage.thenSubmit(table, mutations));
        }

        @Override
        public WriteStage thenSubmit(Collection<Mutation> mutations, long timeout, TimeUnit unit) {
            return propagateCompletion(stage.thenSubmit(table, mutations, timeout, unit));
        }

        @Override
        public WriteStage thenTrySubmit(Collection<Mutation> mutations) {
            return propagateCompletion(stage.thenTrySubmit(table, mutations));
        }

        private WriteStage propagateCompletion(MutationWriteStage nextStage) {
            TableSpecificStage wrapped = new TableSpecificStage(table, nextStage);
            nextStage.whenComplete((x, t) -> {
                if (t == null) {
                    wrapped.complete(x);
                } else {
                    wrapped.completeExceptionally(t);
                }
            });
            return wrapped;
        }

    }

    class AsyncBatchWriterImpl implements AsyncBatchWriter {

        private final String table;
        private final WriterStrategy strategy;
        private final CompletionBarrier barrier;

        AsyncBatchWriterImpl(String table) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
            MultiTableBatchWriter writer = AsyncMultiTableBatchWriterImpl.this.writer;
            BatchWriter bw = writer.getBatchWriter(table);
            this.table = table;
            this.strategy = new SingleTableWriterStrategy(writer, bw, table);
            this.barrier = new CompletionBarrier();
        }

        @Override
        public WriteStage submit(Mutation mutation) throws InterruptedException {
            return propagateCompletionThenTrack(flushTask.submit(createMutation(mutation)));
        }

        @Override
        public WriteStage submit(Mutation mutation, long timeout, TimeUnit unit) throws InterruptedException {
            return propagateCompletionThenTrack(flushTask.submit(createMutation(mutation), timeout, unit));
        }

        @Override
        public WriteStage trySubmit(Mutation mutation) {
            return propagateCompletionThenTrack(flushTask.trySubmit(createMutation(mutation)));
        }

        private FutureSingleMutation createMutation(Mutation mutation) {
            AsyncMultiTableBatchWriter writer = AsyncMultiTableBatchWriterImpl.this;
            return new FutureSingleMutation(writer, strategy, mutation, capacityLimit);
        }

        @Override
        public WriteStage submit(Collection<Mutation> mutations) throws InterruptedException {
            return propagateCompletionThenTrack(flushTask.submit(createMutation(mutations)));
        }

        @Override
        public WriteStage submitMany(Collection<Mutation> mutations, long timeout, TimeUnit unit) throws InterruptedException {
            return propagateCompletionThenTrack(flushTask.submit(createMutation(mutations), timeout, unit));
        }

        @Override
        public WriteStage trySubmitMany(Collection<Mutation> mutations) {
            return propagateCompletionThenTrack(flushTask.trySubmit(createMutation(mutations)));
        }

        private FutureMutationBatch createMutation(Collection<Mutation> mutations) {
            AsyncMultiTableBatchWriter writer = AsyncMultiTableBatchWriterImpl.this;
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

        private WriteStage propagateCompletionThenTrack(MutationWriteStage stage) {
            WriteStage wrapped = propagateCompletion(stage);
            barrier.submit(wrapped);
            return wrapped;
        }

        private WriteStage propagateCompletion(MutationWriteStage nextStage) {
            TableSpecificStage wrapped = new TableSpecificStage(table, nextStage);
            nextStage.whenComplete((x, t) -> {
                if (t == null) {
                    wrapped.complete(x);
                } else {
                    wrapped.completeExceptionally(t);
                }
            });
            return wrapped;
        }

    }

    private static abstract class FutureMutation extends CompletableFuture<Void> implements MutationWriteFuture {

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

    }

    private static abstract class CapacityLimitedFutureMutation extends FutureMutation {

        private final long permits;
        private LongSemaphore capacityLimit;

        CapacityLimitedFutureMutation(long permits, LongSemaphore capacityLimit) {
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
            return true;
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

    private static final class SingleTableWriterStrategy implements WriterStrategy {

        private final String table;

        private MultiTableBatchWriter multiTableBatchWriter;
        private BatchWriter writer;

        public SingleTableWriterStrategy(MultiTableBatchWriter multiTableBatchWriter, BatchWriter writer, String table) {
            this.multiTableBatchWriter = multiTableBatchWriter;
            this.writer = writer;
            this.table = table;
        }

        public BatchWriter getWriter(MultiTableBatchWriter writer) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
            if (this.multiTableBatchWriter != writer) {
                this.multiTableBatchWriter = writer;
                this.writer = multiTableBatchWriter.getBatchWriter(table);
            }
            return this.writer;
        }

    }

    private static final class MultiTableWriterStrategy implements WriterStrategy {

        private final String table;

        public MultiTableWriterStrategy(String table) {
            this.table = table;
        }

        public BatchWriter getWriter(MultiTableBatchWriter writer) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
            return writer.getBatchWriter(table);
        }

    }

    private static final class FutureSingleMutation extends CapacityLimitedFutureMutation {

        private final AsyncMultiTableBatchWriter writer;
        private final WriterStrategy strategy;
        private final Mutation mutation;

        FutureSingleMutation(AsyncMultiTableBatchWriter writer, String table, Mutation mutation, LongSemaphore capacityLimit) {
            this(writer, new MultiTableWriterStrategy(table), mutation, capacityLimit);
        }

        FutureSingleMutation(AsyncMultiTableBatchWriter writer, WriterStrategy strategy, Mutation mutation, LongSemaphore capacityLimit) {
            super(mutation.estimatedMemoryUsed(), capacityLimit);
            this.writer = writer;
            this.strategy = strategy;
            this.mutation = mutation;
        }

        @Override
        public MutationWriteStage thenSubmit(String table, Mutation mutation) {
            return submitTo(writer, this, table, mutation);
        }

        @Override
        public MutationWriteStage thenSubmit(String table, Mutation mutation, long timeout, TimeUnit unit) {
            return submitTo(writer, this, table, mutation, timeout, unit);
        }

        @Override
        public MutationWriteStage thenTrySubmit(String table, Mutation mutation) {
            return trySubmitTo(writer, this, table, mutation);
        }

        @Override
        public MutationWriteStage thenSubmit(String table, Collection<Mutation> mutations) {
            return submitManyTo(writer, this, table, mutations);
        }

        @Override
        public MutationWriteStage thenSubmit(String table, Collection<Mutation> mutations, long timeout, TimeUnit unit) {
            return submitManyTo(writer, this, table, mutations, timeout, unit);
        }

        @Override
        public MutationWriteStage thenTrySubmit(String table, Collection<Mutation> mutations) {
            return trySubmitManyTo(writer, this, table, mutations);
        }

        @Override
        public void submit(MultiTableBatchWriter writer) {
            try {
                strategy.getWriter(writer).addMutation(mutation);
            } catch (Throwable e) {
                completeExceptionally(e);
            }
        }

    }

    private static final class FutureMutationBatch extends CapacityLimitedFutureMutation {

        private final AsyncMultiTableBatchWriter writer;
        private final WriterStrategy strategy;
        private final Collection<Mutation> mutations;

        FutureMutationBatch(AsyncMultiTableBatchWriter writer, String table, Collection<Mutation> mutations, LongSemaphore capacityLimit) {
            this(writer, new MultiTableWriterStrategy(table), mutations, capacityLimit);
        }

        FutureMutationBatch(AsyncMultiTableBatchWriter writer, WriterStrategy strategy, Collection<Mutation> mutations, LongSemaphore capacityLimit) {
            super(computePermits(mutations), capacityLimit);
            this.writer = writer;
            this.strategy = strategy;
            this.mutations = mutations;
        }

        @Override
        public MutationWriteStage thenSubmit(String table, Mutation mutation) {
            return submitTo(writer, this, table, mutation);
        }

        @Override
        public MutationWriteStage thenSubmit(String table, Mutation mutation, long timeout, TimeUnit unit) {
            return submitTo(writer, this, table, mutation, timeout, unit);
        }

        @Override
        public MutationWriteStage thenTrySubmit(String table, Mutation mutation) {
            return trySubmitTo(writer, this, table, mutation);
        }

        @Override
        public MutationWriteStage thenSubmit(String table, Collection<Mutation> mutations) {
            return submitManyTo(writer, this, table, mutations);
        }

        @Override
        public MutationWriteStage thenSubmit(String table, Collection<Mutation> mutations, long timeout, TimeUnit unit) {
            return submitManyTo(writer, this, table, mutations, timeout, unit);
        }

        @Override
        public MutationWriteStage thenTrySubmit(String table, Collection<Mutation> mutations) {
            return trySubmitManyTo(writer, this, table, mutations);
        }

        @Override
        public void submit(MultiTableBatchWriter writer) throws MutationsRejectedException {
            try {
                strategy.getWriter(writer).addMutations(mutations);
            } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
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

        @Override
        long size() {
            return 0L;
        }

        @Override
        void acquire() throws InterruptedException {
        }

        @Override
        boolean acquire(long timeout, TimeUnit unit) throws InterruptedException {
            return true;
        }

        @Override
        public MutationWriteStage thenSubmit(String table, Mutation mutation) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MutationWriteStage thenSubmit(String table, Mutation mutation, long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MutationWriteStage thenTrySubmit(String table, Mutation mutation) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MutationWriteStage thenSubmit(String table, Collection<Mutation> mutations) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MutationWriteStage thenSubmit(String table, Collection<Mutation> mutations, long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MutationWriteStage thenTrySubmit(String table, Collection<Mutation> mutations) {
            throw new UnsupportedOperationException();
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

        private int flushes = 0;

        FlushTask(long batchCapacity, int flushesPerSecondLimit) {
            this.batchCapacityInBytes = batchCapacity;
            this.queue = new LinkedBlockingQueue<>();
            this.batch = new ArrayList<>();
            this.limiter = RateLimiter.create(flushesPerSecondLimit);
        }

        FutureMutation submit(FutureMutation mutation) throws InterruptedException {
            ensureNotShutdown();
            mutation.acquire();
            queue.offer(mutation);
            return mutation;
        }

        FutureMutation submit(FutureMutation mutation, long timeout, TimeUnit unit) throws InterruptedException {
            ensureNotShutdown();
            boolean acquired = mutation.acquire(timeout, unit);
            if (!acquired) {
                mutation.completeExceptionally(SUBMISSION_TIMEOUT);
            }
            queue.offer(mutation);
            return mutation;
        }

        FutureMutation trySubmit(FutureMutation mutation) {
            ensureNotShutdown();
            boolean acquired = mutation.tryAcquire();
            if (!acquired) {
                mutation.completeExceptionally(SUBMISSION_TIMEOUT);
            }
            queue.offer(mutation);
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
                        LOG.debug("Submitting {} mutations -- {} queued", batch.size(), queue.size());
                        for (FutureMutation mutation : batch) {
                            long start = System.nanoTime();
                            mutation.submit(writer);
                            long end = System.nanoTime();
                            long elapsed = end - start;
                            submitTimes.recordValue(elapsed);
                        }
                        limiter.acquire();
                        LOG.debug("Flushing {} mutations", batch.size());
                        long start = System.nanoTime();
                        writer.flush();
                        long end = System.nanoTime();
                        long elapsed = end - start;
                        long millis = TimeUnit.NANOSECONDS.toMillis(elapsed);
                        flushTimes.recordValue(millis);
                        flushes++;
                        completeAll(batch);
                    } catch (MutationsRejectedException e) {
                        failAll(batch, e);
                        LOG.debug("Recreating writer due to rejected mutations", e);
                        try {
                            writer.close();
                        } catch (MutationsRejectedException x) {
                            // suppress because we already failed everything
                        }
                        writer = writerSupplier.get();
                    } catch (RuntimeException e) {
                        handleRuntimeException(batch, e);
                    } finally {
                        batch.clear();
                    }

                    if (LOG.isTraceEnabled() && flushes > 0 && flushes % 100 == 0) {
                        try {
                            traceLogSubmitTimes();
                            traceLogFlushTimes();
                        } catch (UnsupportedEncodingException e) {
                            throw new RuntimeException(e);
                        }
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

        private void traceLogSubmitTimes() throws UnsupportedEncodingException {
            LOG.trace("submit times\n{}", capturePercentileDistribution(submitTimes));
        }

        private void traceLogFlushTimes() throws UnsupportedEncodingException {
            LOG.trace("flush times\n{}", capturePercentileDistribution(flushTimes));
        }

        private String capturePercentileDistribution(Histogram histogram) throws UnsupportedEncodingException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos, true, "utf-8");
            histogram.outputPercentileDistribution(ps, 1d);
            return new String(baos.toByteArray(), StandardCharsets.UTF_8);
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

    private static final class MutationWriteFutureImpl extends ForwardingCompletableFuture<Void> implements MutationWriteFuture {

        private final AsyncMultiTableBatchWriter writer;
        private final CompletableFuture<Void> delegate;

        MutationWriteFutureImpl(AsyncMultiTableBatchWriter writer, CompletableFuture<Void> delegate) {
            super(delegate);
            this.writer = writer;
            this.delegate = delegate;
        }

        @Override
        public MutationWriteStage thenSubmit(String table, Mutation mutation) {
            return submitTo(writer, delegate, table, mutation);
        }

        @Override
        public MutationWriteStage thenSubmit(String table, Mutation mutation, long timeout, TimeUnit unit) {
            return submitTo(writer, delegate, table, mutation, timeout, unit);
        }

        @Override
        public MutationWriteStage thenTrySubmit(String table, Mutation mutation) {
            return trySubmitTo(writer, delegate, table, mutation);
        }

        @Override
        public MutationWriteStage thenSubmit(String table, Collection<Mutation> mutations) {
            return submitManyTo(writer, delegate, table, mutations);
        }

        @Override
        public MutationWriteStage thenSubmit(String table, Collection<Mutation> mutations, long timeout, TimeUnit unit) {
            return submitManyTo(writer, delegate, table, mutations, timeout, unit);
        }

        @Override
        public MutationWriteStage thenTrySubmit(String table, Collection<Mutation> mutations) {
            return trySubmitManyTo(writer, delegate, table, mutations);
        }

    }

    private static MutationWriteFuture submitTo(AsyncMultiTableBatchWriter writer, CompletableFuture<Void> stage,
                                                String table, Mutation mutation) {
        CompletableFuture<Void> f = stage.thenCompose(propagateInterrupt(result -> writer.submit(table, mutation)));
        return new MutationWriteFutureImpl(writer, f);
    }

    private static MutationWriteFuture submitTo(AsyncMultiTableBatchWriter writer, CompletableFuture<Void> stage,
                                                String table, Mutation mutation, long timeout, TimeUnit unit) {
        CompletableFuture<Void> f = stage.thenCompose(
                propagateInterrupt(result -> writer.submit(table, mutation, timeout, unit)));
        return new MutationWriteFutureImpl(writer, f);
    }

    private static MutationWriteFuture trySubmitTo(AsyncMultiTableBatchWriter writer, CompletableFuture<Void> stage,
                                                   String table, Mutation mutation) {
        return new MutationWriteFutureImpl(writer, stage.thenCompose(result -> writer.trySubmit(table, mutation)));
    }

    private static MutationWriteFuture submitManyTo(AsyncMultiTableBatchWriter writer, CompletableFuture<Void> stage,
                                                    String table, Collection<Mutation> mutations) {
        CompletableFuture<Void> f = stage.thenCompose(
                propagateInterrupt(result -> writer.submitMany(table, mutations)));
        return new MutationWriteFutureImpl(writer, f);
    }

    private static MutationWriteFuture submitManyTo(AsyncMultiTableBatchWriter writer, CompletableFuture<Void> stage,
                                                    String table, Collection<Mutation> mutations, long timeout, TimeUnit unit) {
        CompletableFuture<Void> f = stage.thenCompose(
                propagateInterrupt(result -> writer.submitMany(table, mutations, timeout, unit)));
        return new MutationWriteFutureImpl(writer, f);
    }

    private static MutationWriteFuture trySubmitManyTo(AsyncMultiTableBatchWriter writer, CompletableFuture<Void> stage,
                                                       String table, Collection<Mutation> mutations) {
        return new MutationWriteFutureImpl(writer, stage.thenCompose(result -> writer.trySubmitMany(table, mutations)));
    }


    public interface MutationWriteFuture extends Future<Void>, MutationWriteStage {
    }

}
