package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.data.Mutation;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

public interface AsyncMultiTableBatchWriter extends AutoCloseable {

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
     *
     */
    CompletionStage<Void> submitMany(String table, Collection<Mutation> mutations, long timeout, TimeUnit unit) throws InterruptedException;

    /**
     *
     */
    CompletionStage<Void> trySubmitMany(String table, Collection<Mutation> mutations);

    default AsyncBatchWriter getBatchWriter(String table) {
        return new AsyncBatchWriter() {
            @Override
            public CompletionStage<Void> submit(Mutation mutation) throws InterruptedException {
                return AsyncMultiTableBatchWriter.this.submit(table, mutation);
            }

            @Override
            public CompletionStage<Void> submit(Collection<Mutation> mutations) throws InterruptedException {
                return AsyncMultiTableBatchWriter.this.submitMany(table, mutations);
            }

            @Override
            public void await() throws InterruptedException {
                AsyncMultiTableBatchWriter.this.await();
            }

            @Override
            public void await(long timeout, TimeUnit unit) throws InterruptedException {
                AsyncMultiTableBatchWriter.this.await(timeout, unit);
            }

            @Override
            public void close() {
                throw new UnsupportedOperationException("Close the underlying AsyncMultiTableBatchWriter instead.");
            }
        };
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
    void await(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Immediately attempts to stop writing mutations and closes underlying
     * resources.  Mutations that are actively being written may or may not
     * complete.  If completion is required, call await after submission of
     * mutations has stopped and prior to calling close.
     */
    void close();

}
