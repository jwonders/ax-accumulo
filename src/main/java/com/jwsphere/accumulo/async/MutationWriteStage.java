package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.data.Mutation;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public interface MutationWriteStage extends CompletionStage<Void> {

    MutationWriteStage thenSubmit(String table, Mutation mutation);

    MutationWriteStage thenSubmitAsync(String table, Mutation mutation, Executor executor);

    MutationWriteStage thenSubmit(String table, Mutation mutation, long timeout, TimeUnit unit);

    MutationWriteStage thenSubmitAsync(String table, Mutation mutation, Executor executor, long timeout, TimeUnit unit);

    MutationWriteStage thenTrySubmit(String table, Mutation mutation);

    MutationWriteStage thenSubmitMany(String table, Collection<Mutation> mutations);

    MutationWriteStage thenSubmitManyAsync(String table, Collection<Mutation> mutations, Executor executor);

    MutationWriteStage thenSubmitMany(String table, Collection<Mutation> mutations, long timeout, TimeUnit unit);

    MutationWriteStage thenSubmitManyAsync(String table, Collection<Mutation> mutations, Executor executor, long timeout, TimeUnit unit);

    MutationWriteStage thenTrySubmitMany(String table, Collection<Mutation> mutations);

}
