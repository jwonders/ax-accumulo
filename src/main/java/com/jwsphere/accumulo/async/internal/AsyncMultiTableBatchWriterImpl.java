package com.jwsphere.accumulo.async.internal;

import com.google.common.util.concurrent.RateLimiter;
import com.jwsphere.accumulo.async.AsyncMultiTableBatchWriter;
import com.jwsphere.accumulo.async.SubmissionTimeoutException;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Jonathan Wonders
 */
public class AsyncMultiTableBatchWriterImpl implements AsyncMultiTableBatchWriter {

    private static final SubmissionTimeoutException SUBMISSION_TIMEOUT = new SubmissionTimeoutException();

    private final MultiTableBatchWriter writer;

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final FlushTask flushTask;
    private final LongSemaphore capacityLimit = new LongSemaphore(50 * 1024 * 1024);

    /**
     * Creates an async multi table batch writer.  Ownership of the writer
     * is transferred to this instance in order to ensure it is properly closed.
     *
     * @param writer The writer to submit mutations to.
     */
    public AsyncMultiTableBatchWriterImpl(MultiTableBatchWriter writer) {
        this.writer = writer;
        this.flushTask = new FlushTask(8 * 1024, 50);
        this.executorService.submit(flushTask);
    }

    @Override
    public CompletionStage<Void> submit(String table, Mutation mutation) throws InterruptedException {
        return flushTask.submit(new FutureSingleMutation(table, mutation, capacityLimit));
    }

    @Override
    public CompletionStage<Void> submit(String table, Mutation mutation, long timeout, TimeUnit unit) throws InterruptedException {
        return flushTask.submit(new FutureSingleMutation(table, mutation, capacityLimit), timeout, unit);
    }

    @Override
    public CompletionStage<Void> trySubmit(String table, Mutation mutation) {
        return flushTask.trySubmit(new FutureSingleMutation(table, mutation, capacityLimit));
    }

    @Override
    public CompletionStage<Void> submitMany(String table, Collection<Mutation> mutations) throws InterruptedException {
        return flushTask.submit(new FutureMutationBatch(table, mutations, capacityLimit));
    }

    @Override
    public CompletionStage<Void> submitMany(String table, Collection<Mutation> mutations, long timeout, TimeUnit unit) throws InterruptedException {
        return flushTask.submit(new FutureMutationBatch(table, mutations, capacityLimit), timeout, unit);
    }

    @Override
    public CompletionStage<Void> trySubmitMany(String table, Collection<Mutation> mutations) {
        return flushTask.trySubmit(new FutureMutationBatch(table, mutations, capacityLimit));
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

    private static abstract class FutureMutation extends CompletableFuture<Void> {

        abstract long size();

        abstract void acquire() throws InterruptedException;

        abstract void acquire(long timeout, TimeUnit unit) throws InterruptedException;

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

        }

        @Override
        void acquire(long timeout, TimeUnit unit) throws InterruptedException {

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

    private static final class FutureSingleMutation extends CapacityLimitedFutureMutation {

        private final String table;
        private final Mutation mutation;

        FutureSingleMutation(String table, Mutation mutation, LongSemaphore capacityLimit) {
            super(mutation.estimatedMemoryUsed(), capacityLimit);
            this.table = table;
            this.mutation = mutation;
        }

        @Override
        public void submit(MultiTableBatchWriter writer) {
            try {
                writer.getBatchWriter(table).addMutation(mutation);
            } catch (Throwable e) {
                completeExceptionally(e);
            }
        }

    }

    private static final class FutureMutationBatch extends CapacityLimitedFutureMutation {

        private final String table;
        private final Collection<Mutation> mutations;

        FutureMutationBatch(String table, Collection<Mutation> mutations, LongSemaphore capacityLimit) {
            super(computePermits(mutations), capacityLimit);
            this.table = table;
            this.mutations = mutations;
        }

        @Override
        public void submit(MultiTableBatchWriter writer) throws MutationsRejectedException {
            try {
                writer.getBatchWriter(table).addMutations(mutations);
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
        void acquire(long timeout, TimeUnit unit) throws InterruptedException {

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
            return true;
        }

        @Override
        boolean internalComplete() {
            return true;
        }

    }

    private final class FlushTask implements Runnable {

        private final BlockingQueue<FutureMutation> queue;
        private final List<FutureMutation> batch;
        private final RateLimiter limiter;
        private final long batchCapacityInBytes;

        private volatile boolean shutdown = false;

        FlushTask(long batchCapacity, int flushesPerSecondLimit) {
            this.batchCapacityInBytes = batchCapacity;
            this.queue = new LinkedBlockingQueue<>();
            this.batch = new ArrayList<>();
            this.limiter = RateLimiter.create(flushesPerSecondLimit);
        }

        FutureMutation submit(FutureMutation mutation) throws InterruptedException {
            ensureNotShutdown();
            mutation.acquire();
            queue.put(mutation);
            return mutation;
        }

        FutureMutation submit(FutureMutation mutation, long timeout, TimeUnit unit) throws InterruptedException {
            ensureNotShutdown();

            boolean added = queue.offer(mutation, timeout, unit);
            if (!added) {
                mutation.completeExceptionally(SUBMISSION_TIMEOUT);
            }
            return mutation;
        }

        FutureMutation trySubmit(FutureMutation mutation) {
            ensureNotShutdown();
            boolean added = queue.offer(mutation);
            if (!added) {
                mutation.completeExceptionally(SUBMISSION_TIMEOUT);
            }
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
                        for (FutureMutation mutation : batch) {
                            mutation.submit(writer);
                        }
                        limiter.acquire();
                        writer.flush();
                        completeAll(batch);
                    } catch (MutationsRejectedException e) {
                        failAll(batch, e);
                    } catch (RuntimeException e) {
                        handleRuntimeException(batch, e);
                    } finally {
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
                mutation.complete(null);
            }
        }

        private void failAll(List<FutureMutation> batch, Throwable cause) {
            for (FutureMutation mutation : batch) {
                mutation.completeExceptionally(cause);
            }
        }

        void shutdown() {
            this.shutdown = true;
        }

    }

}
