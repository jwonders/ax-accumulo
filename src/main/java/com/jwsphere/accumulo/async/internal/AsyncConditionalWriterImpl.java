package com.jwsphere.accumulo.async.internal;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jwsphere.accumulo.async.AsyncConditionalWriter;
import com.jwsphere.accumulo.async.AsyncConditionalWriterConfig;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ConditionalMutation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class AsyncConditionalWriterImpl implements AsyncConditionalWriter {

    private static final AtomicLong ID = new AtomicLong();

    private final ConditionalWriter writer;
    private final CompletionBarrier barrier;

    private final ThreadPoolExecutor completionExecutor;

    private final long id = ID.getAndIncrement();

    private final LongSemaphore capacityLimit;

    /**
     * Creates an async conditional writer.  Ownership of the supplied writer
     * is transferred to this object.
     */
    public AsyncConditionalWriterImpl(Connector connector, String tableName, AsyncConditionalWriterConfig config) throws TableNotFoundException {
        this.writer = connector.createConditionalWriter(tableName, config.getConditionalWriterConfig());
        this.barrier = new CompletionBarrier();

        this.capacityLimit = new LongSemaphore(config.getMemoryCapacityLimit().orElse(Long.MAX_VALUE));

        ThreadFactory tf = new ThreadFactoryBuilder()
                .setNameFormat("async-cw-" + id + "-%d")
                .build();

        // use an unbounded queue because the semaphore protects against OOME and we want
        // to avoid blocking when submitting tasks for trySubmit methods
        this.completionExecutor = new ThreadPoolExecutor(0, 16, 60L,
                TimeUnit.SECONDS, new LinkedBlockingQueue<>(), tf, new AbortPolicy());
    }

    @Override
    public CompletionStage<ConditionalWriter.Result> submit(ConditionalMutation cm) throws InterruptedException {
        long permits = cm.numBytes();
        capacityLimit.acquire(permits);
        return doSubmit(cm, permits);
    }

    @Override
    public CompletionStage<Result> submit(ConditionalMutation cm, long timeout, TimeUnit unit) throws InterruptedException {
        long permits = cm.numBytes();
        capacityLimit.tryAcquire(permits, timeout, unit);
        return doSubmit(cm, permits);
    }

    @Override
    public CompletionStage<Result> trySubmit(ConditionalMutation cm) {
        long permits = cm.numBytes();
        capacityLimit.tryAcquire(permits);
        return doSubmit(cm, permits);
    }

    private CompletionStage<Result> doSubmit(ConditionalMutation cm, long permits) {
        Iterator<Result> resultIter = getResultIteratorOrReleasePermits(cm, permits);
        CompleteOneTask task = new CompleteOneTask(resultIter, permits);
        completionExecutor.execute(task);
        barrier.submit(task);
        return task;
    }

    @Override
    public CompletionStage<Collection<ConditionalWriter.Result>> submitMany(Collection<ConditionalMutation> mutations) throws InterruptedException {
        long permits = countPermits(mutations);
        capacityLimit.acquire(permits);
        return doSubmitMany(mutations, permits);
    }

    @Override
    public CompletionStage<Collection<Result>> submitMany(Collection<ConditionalMutation> mutations, long timeout, TimeUnit unit) throws InterruptedException {
        long permits = countPermits(mutations);
        capacityLimit.acquire(permits);
        return doSubmitMany(mutations, permits);
    }

    @Override
    public CompletionStage<Collection<Result>> trySubmitMany(Collection<ConditionalMutation> mutations) {
        long permits = countPermits(mutations);
        capacityLimit.tryAcquire(permits);
        return doSubmitMany(mutations, permits);
    }

    private CompletionStage<Collection<Result>> doSubmitMany(Collection<ConditionalMutation> mutations, long permits) {
        Iterator<Result> resultIter = getResultIteratorOrReleasePermits(mutations, permits);
        CompleteManyTask task = new CompleteManyTask(resultIter, mutations.size(), permits);
        completionExecutor.execute(task);
        barrier.submit(task);
        return task;
    }

    private Iterator<Result> getResultIteratorOrReleasePermits(ConditionalMutation cm, long permits) {
        return getResultIteratorOrReleasePermits(Collections.singleton(cm), permits);
    }

    private Iterator<Result> getResultIteratorOrReleasePermits(Collection<ConditionalMutation> mutations, long permits) {
        try {
            return writer.write(mutations.iterator());
        } catch (Throwable e) {
            capacityLimit.release(permits);
            throw e;
        }
    }

    private long countPermits(Collection<ConditionalMutation> mutations) {
        long permits = 0;
        for (ConditionalMutation mutation : mutations) {
            permits += mutation.numBytes();
        }
        return permits;
    }

    @Override
    public void await() throws InterruptedException {
        barrier.await();
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return barrier.await(timeout, unit);
    }

    @Override
    public void close() {
        // the caller is responsible for waiting for writes to complete
        // in which case, shutdownNow will not encounter active or queued tasks.
        completionExecutor.shutdownNow();
        writer.close();
    }

    @Override
    public String toString() {
        return "AsyncConditionalWriterImpl{id=" + id + '}';
    }

    private final class CompleteOneTask extends CompletableFuture<Result> implements Runnable {

        private final Iterator<Result> resultIter;
        private final long permits;

        CompleteOneTask(Iterator<ConditionalWriter.Result> resultIter, long permits) {
            this.resultIter = resultIter;
            this.permits = permits;
        }

        public void run() {
            try {
                complete(getResult());
            } catch (Throwable e) {
                completeExceptionally(e);
            } finally {
                capacityLimit.release(permits);
            }
        }

        private Result getResult() {
            return resultIter.hasNext() ? resultIter.next() : null;
        }

    }

    private final class CompleteManyTask extends CompletableFuture<Collection<Result>> implements Runnable {

        private final Iterator<ConditionalWriter.Result> resultIter;
        private final int count;
        private final long permits;

        CompleteManyTask(Iterator<ConditionalWriter.Result> resultIter, int count, long permits) {
            this.resultIter = resultIter;
            this.count = count;
            this.permits = permits;
        }

        public void run() {
            try {
                List<ConditionalWriter.Result> results = new ArrayList<>(count);
                while (resultIter.hasNext()) {
                    results.add(resultIter.next());
                }
                complete(Collections.unmodifiableCollection(results));
            } catch (Throwable e) {
                completeExceptionally(e);
            } finally {
                capacityLimit.release(permits);
            }
        }

    }

}
