package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.data.ConditionalMutation;

import java.util.Collection;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public interface ConditionalWriteFuture extends ConditionalWriteStage, Future<Result> {

    ConditionalWriteFuture thenSubmit(ConditionalMutation cm);

    ConditionalWriteFuture thenSubmit(ConditionalMutation cm, long timeout, TimeUnit unit);

    ConditionalWriteFuture thenTrySubmit(ConditionalMutation cm);

    ConditionalBatchWriteFuture thenSubmit(Collection<ConditionalMutation> cm);

    ConditionalBatchWriteFuture thenSubmit(Collection<ConditionalMutation> cm, long timeout, TimeUnit unit);

    ConditionalBatchWriteFuture thenTrySubmit(Collection<ConditionalMutation> cm);

}
