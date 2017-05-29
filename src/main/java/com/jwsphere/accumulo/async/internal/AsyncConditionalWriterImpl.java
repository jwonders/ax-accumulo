package com.jwsphere.accumulo.async.internal;

import com.jwsphere.accumulo.async.AsyncConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.data.ConditionalMutation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.CompletableFuture.supplyAsync;

public class AsyncConditionalWriterImpl implements AsyncConditionalWriter {

    private final ConditionalWriter writer;
    private final CompletionBarrier barrier;
    private final ExecutorService executor;

    /**
     * Creates an async conditional writer.  Ownership of the supplied writer
     * is transferred to this object.
     */
    public AsyncConditionalWriterImpl(ConditionalWriter writer) {
        this.writer = writer;
        this.barrier = new CompletionBarrier();
        this.executor = Executors.newCachedThreadPool();
    }

    @Override
    public CompletionStage<ConditionalWriter.Result> submit(ConditionalMutation cm) throws InterruptedException {
        return trySubmit(cm);
    }

    @Override
    public CompletionStage<Result> submit(ConditionalMutation cm, long timeout, TimeUnit unit) throws InterruptedException {
        return trySubmit(cm);
    }

    @Override
    public CompletionStage<Result> trySubmit(ConditionalMutation cm) {
        CompletionStage<ConditionalWriter.Result> stage = supplyAsync(() -> writer.write(cm), executor);
        barrier.submit(stage);
        return stage;
    }

    @Override
    public CompletionStage<Collection<ConditionalWriter.Result>> submitMany(Collection<ConditionalMutation> mutations) throws InterruptedException {
        return trySubmitMany(mutations);
    }

    @Override
    public CompletionStage<Collection<Result>> submitMany(Collection<ConditionalMutation> mutations, long timeout, TimeUnit unit) throws InterruptedException {
        return trySubmitMany(mutations);
    }

    @Override
    public CompletionStage<Collection<Result>> trySubmitMany(Collection<ConditionalMutation> mutations) {
        CompletionStage<Collection<ConditionalWriter.Result>> stage =
                supplyAsync(() -> writeMutations(mutations), executor);
        barrier.submit(stage);
        return stage;
    }


    @Override
    public void await() throws InterruptedException {
        barrier.await();
    }

    @Override
    public void await(long timeout, TimeUnit unit) throws InterruptedException {
        barrier.await(timeout, unit);
    }

    private Collection<ConditionalWriter.Result> writeMutations(Collection<ConditionalMutation> mutations) {
        Iterator<ConditionalWriter.Result> resultIter = writer.write(mutations.iterator());
        List<ConditionalWriter.Result> results = new ArrayList<>(mutations.size());
        while (resultIter.hasNext()) {
            results.add(resultIter.next());
        }
        return Collections.unmodifiableCollection(results);
    }

    @Override
    public void close() {
        // the caller is responsible for waiting for writes to complete
        // in which case, shutdownNow will not encounter active or queued tasks.
        executor.shutdownNow();
        writer.close();
    }

}
