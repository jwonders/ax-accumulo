package com.jwsphere.accumulo.async.internal;

import com.jwsphere.accumulo.async.AsyncConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.data.ConditionalMutation;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

public class ForwardingAsyncConditionalWriter implements AsyncConditionalWriter {

    private final AsyncConditionalWriter writer;

    public ForwardingAsyncConditionalWriter(AsyncConditionalWriter writer) {
        this.writer = writer;
    }

    @Override
    public CompletionStage<Result> submit(ConditionalMutation cm) throws InterruptedException {
        return writer.submit(cm);
    }

    @Override
    public CompletionStage<Result> submit(ConditionalMutation cm, long timeout, TimeUnit unit) throws InterruptedException {
        return writer.submit(cm, timeout, unit);
    }

    @Override
    public CompletionStage<Result> trySubmit(ConditionalMutation cm) {
        return writer.trySubmit(cm);
    }

    @Override
    public CompletionStage<Collection<Result>> submitMany(Collection<ConditionalMutation> mutations) throws InterruptedException {
        return writer.submitMany(mutations);
    }

    @Override
    public CompletionStage<Collection<Result>> submitMany(Collection<ConditionalMutation> mutations, long timeout, TimeUnit unit) throws InterruptedException {
        return writer.submitMany(mutations, timeout, unit);
    }

    @Override
    public CompletionStage<Collection<Result>> trySubmitMany(Collection<ConditionalMutation> mutations) {
        return writer.trySubmitMany(mutations);
    }

    @Override
    public void await() throws InterruptedException {
        writer.await();
    }

    @Override
    public void close() {
        writer.close();
    }

}
