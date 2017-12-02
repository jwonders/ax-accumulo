package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.data.Mutation;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

public interface MutationWriteStage extends CompletionStage<Void> {

    MutationWriteStage thenSubmit(String table, Mutation mutation);

    MutationWriteStage thenSubmit(String table, Mutation mutation, long timeout, TimeUnit unit);

    MutationWriteStage thenTrySubmit(String table, Mutation mutation);

    MutationWriteStage thenSubmit(String table, Collection<Mutation> mutations);

    MutationWriteStage thenSubmit(String table, Collection<Mutation> mutations, long timeout, TimeUnit unit);

    MutationWriteStage thenTrySubmit(String table, Collection<Mutation> mutations);

}
