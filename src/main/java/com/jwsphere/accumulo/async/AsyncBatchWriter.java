package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.data.Mutation;

import java.util.Collection;
import java.util.concurrent.CancellationException;
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
     * Submits a mutation for asynchronous insertion into the table.
     *
     * <p>
     * This method will return following submission, and may block when there is
     * insufficient capacity to accept the mutation.
     *
     * @return A completion stage for the insert.  Successful completion entails
     * the mutation has been committed to the table.  Exceptional completion
     * does not guarantee that the mutation has not been committed.
     */
    WriteStage submit(Mutation mutation) throws InterruptedException;

    /**
     * Submits a mutation for asynchronous insertion into the table.
     *
     * <p>
     * This method will return immediately as submission is performed asynchronously
     * on the provided executor.
     *
     * @return A completion stage for the insert.  Successful completion entails
     * the mutation has been committed to the table.  Exceptional completion
     * does not guarantee that the mutation has not been committed.
     */
    WriteStage submitAsync(Mutation mutation, Executor executor);

    /**
     * Submits a mutation for asynchronous insertion into the table.
     *
     * <p>
     * This method will return following submission, and may block when there is
     * insufficient capacity to accept the mutation.
     *
     * <p>
     * If there is insufficient capacity and the requested timeout elapses, the
     * submission will be cancelled and the resulting completion stage will
     * be completed exceptionally with a {@link CancellationException}.
     *
     * @return A completion stage for the insert.  Successful completion entails
     * the mutation has been committed to the table.  Exceptional completion
     * does not guarantee that the mutation has not been committed.
     */
    WriteStage submit(Mutation mutation, long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Submits a mutation for asynchronous insertion into the table.
     *
     * <p>
     * This method will return immediately following submission, and may block
     * when there is insufficient capacity to accept the mutation.
     *
     * <p>
     * If there is insufficient capacity and the requested timeout elapses, the
     * submission will be cancelled and the resulting completion stage will
     * be completed exceptionally with a {@link CancellationException}.
     *
     * @return A completion stage for the insert.  Successful completion entails
     * the mutation has been committed to the table.  Exceptional completion
     * does not guarantee that the mutation has not been committed.
     */
    WriteStage submitAsync(Mutation mutation, Executor executor, long timeout, TimeUnit unit);

    /**
     * Attempts to submit a mutation for insertion into the table.  This method
     * will always return immediately.
     *
     * <p>
     * If there was insufficient capacity to submit the mutation, the submission will be
     * cancelled and the completion stage will exhibit a {@link CancellationException}.
     *
     * @return A completion stage for the insert.  Successful completion entails
     * that the mutation has been applied to the table with the configured durability.
     */
    WriteStage trySubmit(Mutation mutation);

    /**
     * Submits a collection of mutations for insertion into the table.
     *
     * <p>
     * This method will return immediately as submission is performed asynchronously
     * on the provided executor.
     *
     * @return A completion stage for the insert.  The mutations are tracked
     * collectively such that only when all complete, will the completion stage
     * exhibit a result.
     */
    WriteStage submitMany(Collection<Mutation> mutations) throws InterruptedException;

    /**
     * Submits a collection of mutations for insertion into the table.
     *
     * <p>
     * This method will return immediately as submission is performed asynchronously
     * on the provided executor.
     *
     * @return A completion stage for the insert.  The mutations are tracked
     * collectively such that only when all complete, will the completion stage
     * exhibit a result.
     */
    WriteStage submitManyAsync(Collection<Mutation> mutations, Executor executor);

    /**
     * Submits a collection of mutations for insertion into the table.
     *
     * <p>
     * This method will return immediately after submission, but may block when
     * there is insufficient capacity to accept the mutations.
     *
     * @return A completion stage for the insert.  The mutations are tracked
     * collectively such that only when all complete, will the completion stage
     * exhibit a result.
     */
    WriteStage submitMany(Collection<Mutation> mutations, long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Submits a collection of mutations for insertion into the table.
     *
     * <p>
     * This method will return immediately as submission is performed asynchronously
     * on the provided executor.
     *
     * <p>
     * If there is insufficient capacity and the requested timeout elapses, the
     * submission will be cancelled and the resulting completion stage will
     * be completed exceptionally with a {@link CancellationException}.
     *
     * @return A completion stage for the insert.  The mutations are tracked
     * collectively such that only when all complete, will the completion stage
     * exhibit a result.
     */
    WriteStage submitManyAsync(Collection<Mutation> mutations, Executor executor, long timeout, TimeUnit unit);

    /**
     * Attempts to submit a collection of mutations for insertion into the table.
     * This method will return immediately.
     *
     * <p>
     * If there was insufficient capacity to submit the mutations, the submission will be
     * cancelled and the completion stage will exhibit a {@link CancellationException}.
     *
     * @return A completion stage for the insert.  The mutations are tracked
     * collectively such that only when all complete, will the completion stage
     * exhibit a result.
     */
    WriteStage trySubmitMany(Collection<Mutation> mutations);

    WriteStage asWriteStage(CompletionStage<Void> stage);

    /**
     * Waits until previously submitted mutations have completed.  A mutation is
     * considered to be submitted if there is a happens-before relationship
     * between the return of the corresponding call to submit and the call to
     * await.
     *
     * This method may block indefinitely.
     *
     * @throws InterruptedException If this thread is interrupted while waiting.
     */
    void await() throws InterruptedException;

    /**
     * Waits until previously submitted mutations have completed or the timeout
     * has elapsed, whichever happens first.  A mutation is considered to be
     * submitted if there is a happens-before relationship between the return of
     * the corresponding call to submit and the call to await.
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
