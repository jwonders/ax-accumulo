package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.data.Mutation;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * An asynchronous interface for interacting with a batch writer.
 *
 * @author Jonathan Wonders
 */
public interface AsyncBatchWriter extends AutoCloseable, Awaitable {

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
    WriteStage submit(Mutation mutation) throws InterruptedException;

    WriteStage submitAsync(Mutation mutation, Executor executor);

    /**
     * Asynchronously inserts a mutation into the table.
     */
    WriteStage submit(Mutation mutation, long timeout, TimeUnit unit) throws InterruptedException;

    WriteStage submitAsync(Mutation mutation, Executor executor, long timeout, TimeUnit unit);

    WriteStage trySubmit(Mutation mutation);

    WriteStage submitMany(Collection<Mutation> mutations) throws InterruptedException;

    WriteStage submitManyAsync(Collection<Mutation> mutations, Executor executor);

    WriteStage submitMany(Collection<Mutation> mutations, long timeout, TimeUnit unit) throws InterruptedException;

    WriteStage submitManyAsync(Collection<Mutation> mutations, Executor executor, long timeout, TimeUnit unit);

    WriteStage trySubmitMany(Collection<Mutation> mutations);

    void await() throws InterruptedException;

    boolean await(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Immediately attempts to stop writing mutations and closes underlying
     * resources.  Mutations that are actively being written may or may not
     * complete.  If completion is required, call await after submission of
     * mutations has stopped and prior to calling close.
     */
    void close();

    interface WriteStage extends CompletionStage<Void> {

        WriteStage thenSubmit(Mutation mutation);

        WriteStage thenSubmitAsync(Mutation mutation, Executor executor);

        WriteStage thenSubmit(Mutation mutation, long timeout, TimeUnit unit);

        WriteStage thenSubmitAsync(Mutation mutation, Executor executor, long timeout, TimeUnit unit);

        WriteStage thenTrySubmit(Mutation mutation);

        WriteStage thenSubmitMany(Collection<Mutation> mutations);

        WriteStage thenSubmitManyAsync(Collection<Mutation> mutations, Executor executor);

        WriteStage thenSubmitMany(Collection<Mutation> mutations, long timeout, TimeUnit unit);

        WriteStage thenSubmitManyAsync(Collection<Mutation> mutations, Executor executor, long timeout, TimeUnit unit);

        WriteStage thenTrySubmitMany(Collection<Mutation> mutations);

    }

}
