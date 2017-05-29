package com.jwsphere.accumulo.async.internal;

import com.jwsphere.accumulo.async.AsyncConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.data.ConditionalMutation;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;

public class LimitedCapacityAsyncConditionalWriter extends ForwardingAsyncConditionalWriter {

    private final Semaphore semaphore;

    public LimitedCapacityAsyncConditionalWriter(AsyncConditionalWriter writer, long maxBytesInFlight) {
        super(writer);
        this.semaphore = new Semaphore((int) maxBytesInFlight);
    }

    @Override
    public CompletionStage<Result> submit(ConditionalMutation cm) throws InterruptedException {
        // TODO long semaphore
        int requiredPermits = (int) cm.estimatedMemoryUsed();
        semaphore.acquire(requiredPermits);
        return super.submit(cm);
    }

    @Override
    public CompletionStage<Collection<Result>> submitMany(Collection<ConditionalMutation> mutations) throws InterruptedException {
        return super.submitMany(mutations);
    }

}
