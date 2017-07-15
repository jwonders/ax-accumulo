package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.data.Mutation;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * An asynchronous interface for interacting with a batch writer.
 *
 * @author Jonathan Wonders
 */
public interface AsyncBatchWriter extends AutoCloseable, Awaitable {

    CompletionStage<Void> submit(Mutation mutation) throws InterruptedException;

    CompletionStage<Void> submit(Mutation mutation, long timeout, TimeUnit unit) throws InterruptedException;

    CompletionStage<Void> trySubmit(Mutation mutation);

    CompletionStage<Void> submit(Collection<Mutation> mutations) throws InterruptedException;

    CompletionStage<Void> submitMany(Collection<Mutation> mutations, long timeout, TimeUnit unit) throws InterruptedException;

    CompletionStage<Void> trySubmitMany(Collection<Mutation> mutations);

    void await() throws InterruptedException;

    boolean await(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Immediately attempts to stop writing mutations and closes underlying
     * resources.  Mutations that are actively being written may or may not
     * complete.  If completion is required, call await after submission of
     * mutations has stopped and prior to calling close.
     */
    void close();

}
