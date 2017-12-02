package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.data.ConditionalMutation;

import java.util.Collection;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * A conditional writer that supports asynchronous writes and non-blocking
 * interaction.  This paradigm is well suited for applications that perform
 * many independent writes, especially when there are different contexts
 * associated with each individual write.
 * </p>
 *
 * <p>
 * A web service that might serve many users concurrently is a motivating example.
 * </p>
 *
 * <p>
 * Another goal is to simplify the implementation of multi-mutation transactions
 * by providing a clear API for scheduling dependent mutations to be written
 * sequentially while allowing independent mutations to be written concurrently.
 * </p>
 *
 * @author Jonathan Wonders
 */
public interface AsyncConditionalWriter extends AutoCloseable, Awaitable {

    /**
     * Submits a mutation for asynchronous insertion into the table.  This method
     * will return following submission, and may block when there is insufficient
     * capacity to accept the mutation.
     *
     * @return A completion stage for the insert.  Successful completion entails
     * the mutation has been committed to the table.  Exceptional completion
     * does not guarantee that the mutation has not been committed.
     */
    SingleWriteStage submit(ConditionalMutation cm) throws InterruptedException;

    /**
     * Submits a mutation for asynchronous insertion into the table.  This method
     * will return immediately as submission is performed asynchronously via the
     * provided executor.
     *
     * @return A completion stage for the insert.  Successful completion entails
     * the mutation has been committed to the table.  Exceptional completion
     * does not guarantee that the mutation has not been committed.
     */
    SingleWriteStage submitAsync(ConditionalMutation cm, Executor executor);

    /**
     * Submits a mutation for asynchronous insertion into the table.  This method
     * will return following submission, and may block when there is insufficient
     * capacity to accept the mutation.  If there is insufficient capacity and
     * the provided timeout elapses, the submission will be cancelled and the
     * resulting completion stage will exhibit a {@link CancellationException}.
     *
     * @return A completion stage for the insert.  Successful completion entails
     * the mutation has been committed to the table.  Exceptional completion
     * does not guarantee that the mutation has not been committed.
     */
    SingleWriteStage submit(ConditionalMutation cm, long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Submits a mutation for asynchronous insertion into the table.  This method
     * will return immediately as submission is performed asynchronously via the
     * provided executor.  If there is insufficient capacity and the requested
     * timeout elapses, the submission will be cancelled and the resulting
     * completion stage will exhibit a {@link CancellationException}.
     *
     * @return A completion stage for the insert.  Successful completion entails
     * the mutation has been committed to the table.  Exceptional completion
     * does not guarantee that the mutation has not been committed.
     */
    SingleWriteStage submitAsync(ConditionalMutation cm, Executor executor, long timeout, TimeUnit unit);

    /**
     * Attempts to submit a mutation for insertion into the table.  This method
     * will always return immediately.  If there was insufficient capacity to
     * submit the mutation, the submission will be cancelled and the completion
     * stage will exhibit a {@link CancellationException}.
     *
     * @return A completion stage for the insert.  Successful completion entails
     * that the mutation has been applied to the table with the configured durability.
     */
    SingleWriteStage trySubmit(ConditionalMutation cm);

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
    BatchWriteStage submitMany(Collection<ConditionalMutation> mutations) throws InterruptedException;

    /**
     * Submits a collection of mutations for insertion into the table.  This
     * method will return immediately after submission, but may block when
     * there is insufficient capacity to accept the mutations.
     *
     * @return A completion stage for the insert.  The mutations are tracked
     * collectively such that only when all complete, will the completion stage
     * exhibit a result.
     */
    BatchWriteStage submitMany(Collection<ConditionalMutation> mutations, long timeout, TimeUnit unit) throws InterruptedException;

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
    BatchWriteStage trySubmitMany(Collection<ConditionalMutation> mutations);

    /**
     * Creates a rate limited conditional writer.
     */
    AsyncConditionalWriter withRateLimit(double bytesPerSecond);

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

    /**
     * A stage that represents a possibly asynchronous computation and provides
     * methods for submitting dependent mutations to an async conditional writer
     * upon completion of this stage.  Typically this stage will be created by
     * an {@link AsyncConditionalWriter} and will submit dependent mutations to
     * the same writer.
     *
     * Note that the completion of this write stage happens-before the
     * dependent writes are submitted so one must be careful when chaining
     * write stages and calling {@link AsyncConditionalWriter#await}.
     *
     * In the following example, it is possible and quite common that the call
     * to {@code await()} returns prior to the writing of mutation b.
     *
     * <code>
     * ConditionalMutation a = ...
     * ConditionalMutation b = ...
     *
     * AsyncConditionalWriter writer = ...
     *
     * writer.submit(a).thenSubmit(b);
     * writer.await();
     * </code>
     *
     * The writer is possibly unaware of mutation b when {@code await()} is called
     * since the stage promises not to submit it to the writer until mutation a
     * has been successfully written.
     *
     * To block the thread until the writing of mutation b is complete, one can
     * either use the blocking {@code join} or {@code get} methods on the dependent
     * stage or register a completion action with the dependent stage that will
     * allow the thread to unblock following its completion.  The latter should be
     * preferred when awaiting multiple completion events.
     */
    interface WriteStage<T> extends CompletionStage<T> {

        /**
         * Composes this stage with the stage that represents the writing of
         * another (the supplied) mutation to the same writer.  The subsequent
         * submission may block indefinitely if capacity is unavailable
         */
        SingleWriteStage thenSubmit(ConditionalMutation cm);

        SingleWriteStage thenSubmit(ConditionalMutation cm, long timeout, TimeUnit unit);

        SingleWriteStage thenTrySubmit(ConditionalMutation cm);

        BatchWriteStage thenSubmit(Collection<ConditionalMutation> cm);

        BatchWriteStage thenSubmit(Collection<ConditionalMutation> cm, long timeout, TimeUnit unit);

        BatchWriteStage thenTrySubmit(Collection<ConditionalMutation> cm);

    }

    interface SingleWriteStage extends WriteStage<Result> {}
    interface BatchWriteStage extends WriteStage<Collection<Result>> {}

}
