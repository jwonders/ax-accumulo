package com.jwsphere.accumulo.async.internal;

import com.jwsphere.accumulo.async.AsyncConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.data.ConditionalMutation;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;

public class BoundedAsyncConditionalWriter extends ForwardingAsyncConditionalWriter {

    private final Semaphore semaphore;

    public BoundedAsyncConditionalWriter(AsyncConditionalWriter writer, int maxInFlightMutations) {
        super(writer);
        this.semaphore = new Semaphore(maxInFlightMutations);
    }

    @Override
    public CompletionStage<Result> submit(ConditionalMutation cm) throws InterruptedException {
        semaphore.acquire();
        return super.submit(cm).toCompletableFuture()
                .whenComplete((x, e) -> semaphore.release());
    }

    @Override
    public CompletionStage<Collection<Result>> submitMany(Collection<ConditionalMutation> mutations) throws InterruptedException {
        semaphore.acquire();
        return super.submitMany(mutations).toCompletableFuture()
                .whenComplete((x, e) -> semaphore.release());
    }

}
