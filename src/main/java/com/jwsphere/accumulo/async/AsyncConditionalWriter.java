package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.data.ConditionalMutation;

import java.util.Collection;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;


public interface AsyncConditionalWriter extends AutoCloseable {

    /**
     * Submits a mutation for insertion into the table.  This method will return
     * immediately after submission, but may block when there is insufficient
     * capacity to accept the mutation.
     *
     * @return A completion stage for the insert.  Successful completion entails
     * that the mutation has been applied to the table with the configured durability.
     */
    CompletionStage<Result> submit(ConditionalMutation cm) throws InterruptedException;

    /**
     * Submits a mutation for insertion into the table.  This method will return
     * immediately after submission, but may block when there is insufficient
     * capacity to accept the mutation.  If there is insufficient capacity and
     * the requested timeout elapses, the submission will be cancelled and the
     * resulting completion stage will exhibit a {@link CancellationException}.
     *
     * @return A completion stage for the insert.  Successful completion entails
     * that the mutation has been applied to the table with the configured durability.
     */
    CompletionStage<Result> submit(ConditionalMutation cm, long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Attempts to submit a mutation for insertion into the table.  This method
     * will always return immediately.  If there was insufficient capacity to
     * submit the mutation, the submission will be cancelled and the completion
     * stage will exhibit a {@link CancellationException}.
     *
     * @return A completion stage for the insert.  Successful completion entails
     * that the mutation has been applied to the table with the configured durability.
     */
    CompletionStage<Result> trySubmit(ConditionalMutation cm);

    /**
     * Submits a collection of mutations for insertion into the table.  This
     * method will return immediately after submission, but may block when
     * there is insufficient capacity to accept the mutations.  If there is
     * insufficient capacity and the requested timeout elapses, the submission
     * will be cancelled and the resulting completion stage will exhibit a
     * {@link CancellationException}.
     *
     * @return A completion stage for the insert.  The mutations are tracked
     * collectively such that only when all complete, will the completion stage
     * exhibit a result.
     */
    CompletionStage<Collection<Result>> submitMany(Collection<ConditionalMutation> mutations) throws InterruptedException;

    /**
     * Submits a collection of mutations for insertion into the table.  This
     * method will return immediately after submission, but may block when
     * there is insufficient capacity to accept the mutations.
     *
     * @return A completion stage for the insert.  The mutations are tracked
     * collectively such that only when all complete, will the completion stage
     * exhibit a result.
     */
    CompletionStage<Collection<Result>> submitMany(Collection<ConditionalMutation> mutations, long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Attempts to submit a collection of mutations for insertion into the table.
     * This method will return immediately.  If there was insufficient capacity to
     * submit the mutations, the submission will be cancelled and the completion
     * stage will exhibit a {@link CancellationException}.
     *
     * @return A completion stage for the insert.  The mutations are tracked
     * collectively such that only when all complete, will the completion stage
     * exhibit a result.
     */
    CompletionStage<Collection<Result>> trySubmitMany(Collection<ConditionalMutation> mutations);

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
    void await(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Immediately attempts to stop writing mutations and closes underlying
     * resources.  Mutations that are actively being written may or may not
     * complete.  If completion is required, call await after submission of
     * mutations has stopped and prior to calling close.
     */
    void close();

}
