package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Collection;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface AsyncMultiTableConditionalWriter {

    /**
     * Submits a mutation for insertion into the table.  This method will return
     * immediately after submission, but may block when there is insufficient
     * capacity to accept the mutation.
     *
     * @return A completion stage for the insert.  Successful completion entails
     * that the mutation has been applied to the table with the configured durability.
     */
    WriteStage submit(String table, Authorizations auth, ConditionalMutation mutation) throws InterruptedException;

    /**
     * Submits a mutation for insertion into the table.  This method will return
     * immediately after submission, but may block when there is insufficient
     * capacity to accept the mutation.
     *
     * @return A completion stage for the insert.  Successful completion entails
     * that the mutation has been applied to the table with the configured durability.
     */
    WriteStage submitAsync(String table, Authorizations auth, ConditionalMutation mutation, Executor executor);

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
    WriteStage submit(String table, Authorizations auth, ConditionalMutation mutation, long timeout, TimeUnit unit) throws InterruptedException;

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
    WriteStage submitAsync(String table, Authorizations auth, ConditionalMutation mutation, Executor executor, long timeout, TimeUnit unit);

    /**
     * Attempts to submit a mutation for insertion into the table.  This method
     * will always return immediately.  If there was insufficient capacity to
     * submit the mutation, the submission will be cancelled and the completion
     * stage will exhibit a {@link CancellationException}.
     *
     * @return A completion stage for the insert.  Successful completion entails
     * that the mutation has been applied to the table with the configured durability.
     */
    WriteStage trySubmit(String table, ConditionalMutation mutation);

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
    WriteStage submitMany(String table, Collection<ConditionalMutation> mutations) throws InterruptedException;

    WriteStage submitManyAsync(String table, Collection<ConditionalMutation> mutations, Executor executor);

    /**
     * Submits a collection of mutations for insertion into the table.  This
     * method will return immediately after submission, but may block when
     * there is insufficient capacity to accept the mutations.
     *
     * @return A completion stage for the insert.  The mutations are tracked
     * collectively such that only when all complete, will the completion stage
     * exhibit a result.
     */
    WriteStage submitMany(String table, Collection<ConditionalMutation> mutations, long timeout, TimeUnit unit) throws InterruptedException;

    WriteStage submitManyAsync(String table, Collection<ConditionalMutation> mutations, Executor executor, long timeout, TimeUnit unit);

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
    WriteStage trySubmitMany(String table, Collection<ConditionalMutation> mutations);

    AsyncConditionalWriter getConditionalWriter(String table, Authorizations auth);

    interface WriteStage<T> extends CompletionStage<T> {

        /**
         * Composes this stage with the stage that represents the writing of
         * another (the supplied) mutation to the same writer.  The subsequent
         * submission may block indefinitely if capacity is unavailable
         */
        SingleWriteStage thenSubmit(String table, Authorizations auth, ConditionalMutation cm);

        SingleWriteStage thenSubmitAsync(String table, Authorizations auth, ConditionalMutation cm, Executor executor);

        SingleWriteStage thenSubmit(String table, Authorizations auth, ConditionalMutation cm, long timeout, TimeUnit unit);

        SingleWriteStage thenSubmitAsync(String table, Authorizations auth, ConditionalMutation cm, Executor executor, long timeout, TimeUnit unit);

        SingleWriteStage thenTrySubmit(String table, Authorizations auth, ConditionalMutation cm);

        BatchWriteStage thenSubmitMany(String table, Authorizations auth, Collection<ConditionalMutation> cm);

        BatchWriteStage thenSubmitManyAsync(String table, Authorizations auth, Collection<ConditionalMutation> cm, Executor executor);

        BatchWriteStage thenSubmitMany(String table, Authorizations auth, Collection<ConditionalMutation> cm, long timeout, TimeUnit unit);

        BatchWriteStage thenSubmitManyAsync(String table, Authorizations auth, Collection<ConditionalMutation> cm, Executor executor, long timeout, TimeUnit unit);

        BatchWriteStage thenTrySubmit(String table, Authorizations auth, Collection<ConditionalMutation> cm);

        @Override
        <U> WriteStage<U> handle(BiFunction<? super T, Throwable, ? extends U> fn);

        @Override
        <U> WriteStage<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn);

        @Override
        <U> WriteStage<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn, Executor executor);

        @Override
        <U> WriteStage<U> thenApply(Function<? super T, ? extends U> fn);

        @Override
        <U> WriteStage<U> thenApplyAsync(Function<? super T, ? extends U> fn);

        @Override
        <U> WriteStage<U> thenApplyAsync(Function<? super T, ? extends U> fn, Executor executor);

        @Override
        <U> WriteStage<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn);

        @Override
        <U> WriteStage<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn);

        @Override
        <U> WriteStage<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn, Executor executor);

        @Override
        WriteStage<T> whenComplete(BiConsumer<? super T, ? super Throwable> action);

        @Override
        WriteStage<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action);

        @Override
        WriteStage<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action, Executor executor);

    }

    interface SingleWriteStage extends WriteStage<Result> {}

    interface BatchWriteStage extends WriteStage<Collection<Result>> {}

}
