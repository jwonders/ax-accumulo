package com.jwsphere.accumulo.async.internal;

import com.jwsphere.accumulo.async.AsyncMultiTableBatchWriter;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AsyncMultiTableBatchWriterImpl implements AsyncMultiTableBatchWriter {

    private final MultiTableBatchWriter writer;

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final FlushTask flushTask;

    /**
     * Creates an async multi table batch writer.  Ownership of the writer
     * is transferred to this instance in order to ensure it is properly closed.
     *
     * @param writer The writer to submit mutations to.
     */
    public AsyncMultiTableBatchWriterImpl(MultiTableBatchWriter writer) {
        this.writer = writer;
        this.flushTask = new FlushTask(8 * 1024);
        this.executorService.submit(flushTask);
    }

    @Override
    public CompletionStage<Void> submit(String table, Mutation mutation) throws InterruptedException {
        return flushTask.submit(new FutureSingleMutation(table, mutation));
    }

    @Override
    public CompletionStage<Void> submit(String table, Collection<Mutation> mutations) throws InterruptedException {
        return flushTask.submit(new FutureMutationBatch(table, mutations));
    }

    @Override
    public void await() throws InterruptedException {
        flushTask.submit(new Await()).join();
    }

    @Override
    public void await(long timeout, TimeUnit unit) throws InterruptedException {
        try {
            flushTask.submit((new Await())).get(timeout, unit);
        } catch (ExecutionException e) {
            throw new CompletionException(e);
        } catch (TimeoutException e) {
            // normal behavior
        }
    }

    @Override
    public void shutdown() {
        // request that the flush task stops after any active flush completes
        flushTask.shutdown();

        // assuming the callers have stopped submitting tasks and there
        // is not an issue preventing the completion of tasks, interruption
        // is not necessary to achieve an orderly shutdown

        executorService.shutdown();
    }

    @Override
    public void shutdownNow() {
        executorService.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return executorService.isShutdown();
    }

    @Override
    public void awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        executorService.awaitTermination(timeout, unit);
    }

    @Override
    public boolean isTerminated() {
        return executorService.isTerminated();
    }

    private static abstract class FutureMutation extends CompletableFuture<Void> {

        abstract void submit(MultiTableBatchWriter writer) throws MutationsRejectedException;

    }

    private static final class FutureSingleMutation extends FutureMutation {

        private final String table;
        private final Mutation mutation;

        FutureSingleMutation(String table, Mutation mutation) {
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

    private static final class FutureMutationBatch extends FutureMutation {

        private final String table;
        private final Collection<Mutation> mutations;

        FutureMutationBatch(String table, Collection<Mutation> mutations) {
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

    }

    private static final class Await extends FutureMutation {

        @Override
        void submit(MultiTableBatchWriter writer) {
            // nothing to submit
        }

    }

    private final class FlushTask implements Runnable {

        private final BlockingQueue<FutureMutation> queue;
        private final List<FutureMutation> batch;
        private final int capacity;

        private volatile boolean shutdown = false;

        FlushTask(int capacity) {
            // ideally capacity would be based on memory
            this.capacity = capacity;
            this.queue = new ArrayBlockingQueue<>(capacity);
            this.batch = new ArrayList<>(capacity);
        }

        FutureMutation submit(FutureMutation mutation) throws InterruptedException {
            if (shutdown) {
                throw new IllegalStateException("Cannot submit mutations after a shutdown.");
            }
            queue.put(mutation);
            return mutation;
        }

        @Override
        public void run() {
            try {
                while (!Thread.currentThread().isInterrupted() && !shutdown) {
                    try {
                        batch.add(queue.take());
                    } catch (InterruptedException e) {
                        return;
                    }
                    queue.drainTo(batch, capacity - 1);
                    try {
                        for (FutureMutation mutation : batch) {
                            mutation.submit(writer);
                        }
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
