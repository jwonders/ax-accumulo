package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.data.Mutation;

import java.util.Collection;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * An asynchronous interface for interacting with a {@code MultiTableBatchWriter}
 *
 * @author Jonathan Wonders
 */
public interface AsyncMultiTableBatchWriter extends AutoCloseable, Awaitable {

    /**
     * Submits a mutation for insertion into the table.  This method will return
     * immediately after submission, but may block when there is insufficient
     * capacity to accept the mutation.
     *
     * @return A completion stage for the insert.  Successful completion entails
     * that the mutation has been applied to the table with the configured durability.
     */
    WriteStage submit(String table, Mutation mutation) throws InterruptedException;

    /**
     * Submits a mutation for insertion into the table.  This method will return
     * immediately after submission, but may block when there is insufficient
     * capacity to accept the mutation.
     *
     * @return A completion stage for the insert.  Successful completion entails
     * that the mutation has been applied to the table with the configured durability.
     */
    WriteStage submitAsync(String table, Mutation mutation, Executor executor);

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
    WriteStage submit(String table, Mutation mutation, long timeout, TimeUnit unit) throws InterruptedException;

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
    WriteStage submitAsync(String table, Mutation mutation, Executor executor, long timeout, TimeUnit unit);

    /**
     * Attempts to submit a mutation for insertion into the table.  This method
     * will always return immediately.  If there was insufficient capacity to
     * submit the mutation, the submission will be cancelled and the completion
     * stage will exhibit a {@link CancellationException}.
     *
     * @return A completion stage for the insert.  Successful completion entails
     * that the mutation has been applied to the table with the configured durability.
     */
    WriteStage trySubmit(String table, Mutation mutation);

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
    WriteStage submitMany(String table, Collection<Mutation> mutations) throws InterruptedException;

    WriteStage submitManyAsync(String table, Collection<Mutation> mutations, Executor executor);

    /**
     * Submits a collection of mutations for insertion into the table.  This
     * method will return immediately after submission, but may block when
     * there is insufficient capacity to accept the mutations.
     *
     * @return A completion stage for the insert.  The mutations are tracked
     * collectively such that only when all complete, will the completion stage
     * exhibit a result.
     */
    WriteStage submitMany(String table, Collection<Mutation> mutations, long timeout, TimeUnit unit) throws InterruptedException;

    WriteStage submitManyAsync(String table, Collection<Mutation> mutations, Executor executor, long timeout, TimeUnit unit);

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
    WriteStage trySubmitMany(String table, Collection<Mutation> mutations);

    AsyncBatchWriter getBatchWriter(String table);

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

    interface WriteStage extends CompletionStage<Void> {

        WriteStage thenSubmit(String table, Mutation mutation);

        WriteStage thenSubmitAsync(String table, Mutation mutation, Executor executor);

        WriteStage thenSubmit(String table, Mutation mutation, long timeout, TimeUnit unit);

        WriteStage thenSubmitAsync(String table, Mutation mutation, Executor executor, long timeout, TimeUnit unit);

        WriteStage thenTrySubmit(String table, Mutation mutation);

        WriteStage thenSubmitMany(String table, Collection<Mutation> mutations);

        WriteStage thenSubmitManyAsync(String table, Collection<Mutation> mutations, Executor executor);

        WriteStage thenSubmitMany(String table, Collection<Mutation> mutations, long timeout, TimeUnit unit);

        WriteStage thenSubmitManyAsync(String table, Collection<Mutation> mutations, Executor executor, long timeout, TimeUnit unit);

        WriteStage thenTrySubmitMany(String table, Collection<Mutation> mutations);

    }

    interface Listener {

        FlushEvent startFlushEvent();

        default void recordSubmission(long writerId) {
        }

        default void recordWaitTime(long writerId, long duration, TimeUnit unit) {
        }

        default void recordSubmitTime(long writerId, long duration, TimeUnit unit) {
        }

        default void recordFlushLatency(long writerId, long duration, TimeUnit unit) {
        }

        default void recordWriteLatency(long writerId, long duration, TimeUnit unit) {
        }

        default void recordSubmissionTimedOut(long writerId) {
        }

        default void recordFlush(long writerId) {
        }

    }

    interface FlushEvent extends AutoCloseable {

        default void recordBatchEstimatedMemory(long numBytes) {
        }

        default void recordBatchSize(int numMutations) {
        }

        default void recordRemainingQueueDepth(int size) {
        }

        default void beforeRateLimiting() {
        }

        default void afterRateLimiting() {
        }

        default void beforeFlush() {
        }

        default void afterFlush() {
        }

        default void close() {
        }

    }

    interface ListenerFactory {

        Listener create(long writerId);

    }


}
