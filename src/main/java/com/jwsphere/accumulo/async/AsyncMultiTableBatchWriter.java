package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.data.Mutation;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * An asynchronous interface for interacting with a {@code MultiTableBatchWriter}
 *
 * @author Jonathan Wonders
 */
public interface AsyncMultiTableBatchWriter extends AutoCloseable, Awaitable {

    /**
     * Asynchronously inserts a mutation into the table.
     *
     * This method will block if no space is available in the writer.
     *
     * @return A completion stage representing the operation that will complete
     * when the mutation is known to have been applied to the table.  Take care
     * that dependent completion stages do not block indefinitely as might be
     * the case if chaining submissions.
     */
    CompletionStage<Void> submit(String table, Mutation mutation) throws InterruptedException;

    /**
     * Asynchronously inserts a mutation into the table.
     */
    CompletionStage<Void> submit(String table, Mutation mutation, long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Attempts to submit the mutation for insert into the table.
     */
    CompletionStage<Void> trySubmit(String table, Mutation mutation);

    /**
     * Asynchronously inserts a collection of mutations into the table.
     *
     * This method will block if no space is available in the writer.
     *
     * @return A completion stage representing the operation that will complete
     * when all mutations are known to have been applied to the table.  Take care
     * that dependent completion stages do not block indefinitely as might be
     * the case if chaining submissions.
     */
    CompletionStage<Void> submitMany(String table, Collection<Mutation> mutations) throws InterruptedException;

    /**
     * Asynchronously inserts a collection of mutations into the table.
     *
     * This method will block until the timeout has elapsed if no space
     * is available in the writer.
     *
     * @return A completion stage representing the operation that will complete
     * when all mutations are known to have been applied to the table.  Take care
     * that dependent completion stages do not block indefinitely as might be
     * the case if chaining submissions.
     */
    CompletionStage<Void> submitMany(String table, Collection<Mutation> mutations, long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Asynchronously inserts a collection of mutations into the table.
     *
     * This method will either immediately schedule the mutations for insertion
     * or immediately return a completion stage that has completed exceptionally
     * with a {@link java.util.concurrent.CancellationException}.
     *
     * @return A completion stage representing the operation that will complete
     * when all mutations are known to have been applied to the table.  Take care
     * that dependent completion stages do not block indefinitely as might be
     * the case if chaining submissions.
     */
    CompletionStage<Void> trySubmitMany(String table, Collection<Mutation> mutations);

    default AsyncBatchWriter getBatchWriter(String table) {
        return new DefaultAsyncBatchWriter(this, table);
    }

    /**
     * Waits until previously submitted mutations have been written.
     *
     * @throws InterruptedException If this thread is interrupted while waiting.
     */
    void await() throws InterruptedException;

    /**
     * Waits until either previously submitted mutations have been written or
     * the timeout has elapsed.
     *
     * @throws InterruptedException If this thread is interrupted while waiting.
     */
    boolean await(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Immediately attempts to stop writing mutations and closes underlying
     * resources.  Mutations that are actively being written may or may not
     * complete.  If completion is required, call await after submission of
     * mutations has stopped and prior to calling close.
     */
    void close();

}

final class DefaultAsyncBatchWriter implements AsyncBatchWriter {

    private final AsyncMultiTableBatchWriter writer;
    private final String table;

    DefaultAsyncBatchWriter(AsyncMultiTableBatchWriter writer, String table) {
        this.writer = writer;
        this.table = table;
    }

    @Override
    public CompletionStage<Void> submit(Mutation mutation) throws InterruptedException {
        return writer.submit(table, mutation);
    }

    @Override
    public CompletionStage<Void> submit(Mutation mutation, long timeout, TimeUnit unit) throws InterruptedException {
        return writer.submit(table, mutation, timeout, unit);
    }

    @Override
    public CompletionStage<Void> trySubmit(Mutation mutation) {
        return writer.trySubmit(table, mutation);
    }

    @Override
    public CompletionStage<Void> submit(Collection<Mutation> mutations) throws InterruptedException {
        return writer.submitMany(table, mutations);
    }

    @Override
    public CompletionStage<Void> submitMany(Collection<Mutation> mutations, long timeout, TimeUnit unit) throws InterruptedException {
        return writer.submitMany(table, mutations, timeout, unit);
    }

    @Override
    public CompletionStage<Void> trySubmitMany(Collection<Mutation> mutations) {
        return writer.trySubmitMany(table, mutations);
    }

    @Override
    public void await() throws InterruptedException {
        writer.await();
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return writer.await(timeout, unit);
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Close the underlying AsyncMultiTableBatchWriter instead.");
    }

}