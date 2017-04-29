package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.data.Mutation;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

public interface AsyncBatchWriter extends AutoCloseable {

    CompletionStage<Void> submit(Mutation mutation) throws InterruptedException;

    CompletionStage<Void> submit(Collection<Mutation> mutations) throws InterruptedException;

    void await() throws InterruptedException;

    void await(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Immediately attempts to stop writing mutations and closes underlying
     * resources.  Mutations that are actively being written may or may not
     * complete.  If completion is required, call await after submission of
     * mutations has stopped and prior to calling close.
     */
    void close();

}
