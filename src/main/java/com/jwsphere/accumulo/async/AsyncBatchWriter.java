package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.data.Mutation;

import java.util.Collection;
import java.util.concurrent.CompletionStage;

public interface AsyncBatchWriter {

    CompletionStage<Void> submit(Mutation mutation) throws InterruptedException;

    CompletionStage<Void> submit(Collection<Mutation> mutations) throws InterruptedException;

}
