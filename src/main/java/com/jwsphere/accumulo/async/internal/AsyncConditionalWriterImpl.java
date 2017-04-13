package com.jwsphere.accumulo.async.internal;

import com.jwsphere.accumulo.async.AsyncConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.data.ConditionalMutation;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

public class AsyncConditionalWriterImpl implements AsyncConditionalWriter {

    public static final int PERMITS = 10_000;

    private final ConditionalWriter writer;
    private final Semaphore semaphore;
    private final CompletionTracker tracker;
    private final ExecutorService executor;

    /**
     * Creates an async conditional writer.
     *
     * TODO - It is not obvious how or when to close the writer.
     *   There is not an easy way for this object to know when to close the writer.
     *   Nor is there a good way for the async connector to know when to close the
     *   writer.  It may be possible to use the tracker from a background thread
     *   to wait for tasks to complete upon a shutdown and subsequently close the
     *   writer, but this seems overly complicated.  Another option would be to
     *   allow the tracker to execute tasks upon completion in which case the thread
     *   that the last task completes on would close the writer.  Another option is
     *   to submit the task asynchronously on the ForkJoinPool.commonPool().
     *
     * @param writer
     */
    public AsyncConditionalWriterImpl(ConditionalWriter writer) {
        this.writer = writer;
        this.semaphore = new Semaphore(PERMITS);
        this.tracker = new CompletionTracker();
        this.executor = Executors.newCachedThreadPool();
        writer.close();
    }

    @Override
    public CompletionStage<ConditionalWriter.Result> submit(ConditionalMutation cm) throws InterruptedException {
        ensureAlive();
        semaphore.acquire();
        CompletionStage<ConditionalWriter.Result> stage =
                CompletableFuture.supplyAsync(() -> writeMutation(cm), executor);
        tracker.submit(stage);
        return stage;
    }

    @Override
    public CompletionStage<Collection<ConditionalWriter.Result>> submitMany(Collection<ConditionalMutation> mutations) throws InterruptedException {
        if (mutations.size() > PERMITS) {
            throw new IllegalArgumentException("Insufficient permits to complete operation.");
        }
        ensureAlive();
        semaphore.acquire(mutations.size());
        CompletionStage<Collection<ConditionalWriter.Result>> stage =
                CompletableFuture.supplyAsync(() -> writeMutations(mutations), executor);
        tracker.submit(stage);
        return stage;
    }

    @Override
    public void await() throws InterruptedException {
        tracker.await();
    }

    private ConditionalWriter.Result writeMutation(ConditionalMutation cm) {
        try {
            return writer.write(cm);
        } finally {
            semaphore.release();
        }
    }

    private Collection<ConditionalWriter.Result> writeMutations(Collection<ConditionalMutation> mutations) {
        try {
            Iterator<ConditionalWriter.Result> resultIter = writer.write(mutations.iterator());
            List<ConditionalWriter.Result> results = new ArrayList<>();
            while (resultIter.hasNext()) {
                results.add(resultIter.next());
            }
            return Collections.unmodifiableCollection(results);
        } finally {
            semaphore.release(mutations.size());
        }
    }

    private void ensureAlive() {
        if (executor.isShutdown()) {
            throw new IllegalStateException("Cannot submit operations after shutdown.");
        }
    }

    @Override
    public void shutdown() {
        executor.shutdown();
    }

    @Override
    public void shutdownNow() {
        executor.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return executor.isShutdown();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return executor.awaitTermination(timeout, unit);
    }

    @Override
    public boolean isTerminated() {
        return executor.isTerminated();
    }

}
