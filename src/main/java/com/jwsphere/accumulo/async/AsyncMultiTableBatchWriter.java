package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.data.Mutation;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

public interface AsyncMultiTableBatchWriter {

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
     * Asynchronously inserts a collection of mutations into the table.
     *
     * This method will block if no space is available in the writer.
     *
     * @return A completion stage representing the operation that will complete
     * when all mutations are known to have been applied to the table.  Take care
     * that dependent completion stages do not block indefinitely as might be
     * the case if chaining submissions.
     */
    CompletionStage<Void> submit(String table, Collection<Mutation> mutations) throws InterruptedException;

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
     * Initiates an orderly shutdown where all mutations being actively written
     * will be allowed to complete, but pending mutations will not be written.
     */
    void shutdown();

    /**
     * Attempts to immediately stop writing mutations.  Mutations being actively
     * written may or may not complete and pending mutations will not be written.
     */
    void shutdownNow();

    /**
     * Returns whether or not the writer has been shut down.
     */
    boolean isShutdown();

    /**
     * Waits for mutations to complete or until the timeout has elapsed.
     */
    void awaitTermination(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Returns {@code true} if all mutations have completed (possibly exceptionally)
     * following a shut down.
     */
    boolean isTerminated();

}
