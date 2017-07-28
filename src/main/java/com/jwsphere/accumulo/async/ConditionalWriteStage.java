package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.data.ConditionalMutation;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

public interface ConditionalWriteStage extends CompletionStage<Result> {

    ConditionalWriteStage thenSubmit(ConditionalMutation cm);

    ConditionalWriteStage thenSubmit(ConditionalMutation cm, long timeout, TimeUnit unit);

    ConditionalWriteStage thenTrySubmit(ConditionalMutation cm);

    ConditionalBatchWriteStage thenSubmit(Collection<ConditionalMutation> cm);

    ConditionalBatchWriteStage thenSubmit(Collection<ConditionalMutation> cm, long timeout, TimeUnit unit);

    ConditionalBatchWriteStage thenTrySubmit(Collection<ConditionalMutation> cm);

}
