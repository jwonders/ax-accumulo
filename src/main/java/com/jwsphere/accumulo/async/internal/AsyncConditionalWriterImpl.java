package com.jwsphere.accumulo.async.internal;

import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jwsphere.accumulo.async.AsyncConditionalWriter;
import com.jwsphere.accumulo.async.AsyncConditionalWriterConfig;
import com.jwsphere.accumulo.async.CapacityExceededException;
import com.jwsphere.accumulo.async.SubmissionTimeoutException;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The asynchronous conditional writer allows a bounded size of mutations to
 * be in the process of being written to Accumulo and informs the submitter upon
 * completion or error.
 *
 * The number of in-flight mutations is bounded since the underlying conditional
 * writer uses an unbounded queue and does not protect against out of memory
 * errors.  Mutations are submitted to the underlying writer in the caller's
 * thread since the writer's internal blocking queue is unbounded and should
 * never block.  This assumption is important for the non-blocking methods to
 * exhibit proper behavior (e.g. ${code trySubmit}, ${code trySubmitMany})
 * since the writer does not otherwise have a non-blocking submission method.
 *
 * Note: The conditional writer does incur some per-mutation overhead due to
 * copying and retaining in-flight mutations in a queue.  The capacity limit
 * should be much smaller the the JVM heap and be sized based on how much data
 * needs to be batched to get acceptable throughput.  Higher capacities will
 * likely incur higher latencies when saturated.
 */
public final class AsyncConditionalWriterImpl implements AsyncConditionalWriter {

    private static final SubmissionTimeoutException SUBMISSION_TIMEOUT = new SubmissionTimeoutException();

    private static final AtomicLong ID = new AtomicLong();

    private final ConditionalWriter writer;
    private final CompletionBarrier barrier;

    private final ExecutorService completionExecutor;

    private final long id = ID.getAndIncrement();

    private final long capacityLimit;
    private final LongSemaphore capacityLimiter;
    private final RateLimiter rateLimiter;

    /**
     * Creates an async conditional writer.
     */
    public AsyncConditionalWriterImpl(Connector connector, String tableName, AsyncConditionalWriterConfig config) throws TableNotFoundException {
        this.writer = connector.createConditionalWriter(tableName, config.getConditionalWriterConfig());
        this.barrier = new CompletionBarrier();
        this.completionExecutor = defaultExecutor();
        this.capacityLimit = config.getMemoryCapacityLimit().orElse(Long.MAX_VALUE);
        this.capacityLimiter = new LongSemaphore(capacityLimit);
        this.rateLimiter = null;
    }

    /**
     * Creates an async conditional writer.
     */
    private AsyncConditionalWriterImpl(ConditionalWriter writer, ExecutorService completionExecutor,
                                       long capacityLimit, LongSemaphore capacityLimiter, RateLimiter rateLimiter) {
        this.writer = writer;
        this.barrier = new CompletionBarrier();
        this.completionExecutor = completionExecutor;
        this.capacityLimit = capacityLimit;
        this.capacityLimiter = capacityLimiter;
        this.rateLimiter = rateLimiter;
    }

    public AsyncConditionalWriter withRateLimit(double bytesPerSecond) {
        RateLimiter rateLimiter = RateLimiter.create(bytesPerSecond);
        return new AsyncConditionalWriterImpl(writer, completionExecutor, capacityLimit, capacityLimiter, rateLimiter);
    }

    @Override
    public CompletionStage<ConditionalWriter.Result> submit(ConditionalMutation cm) throws InterruptedException {
        long permits = cm.numBytes();
        if (permits > capacityLimit) {
            return MoreCompletableFutures.immediatelyFailed(new CapacityExceededException(permits, capacityLimit));
        }
        obeyRateLimit(permits);
        capacityLimiter.acquire(permits);
        return doSubmit(cm, permits);
    }

    @Override
    public CompletionStage<Result> submit(ConditionalMutation cm, long timeout, TimeUnit unit) throws InterruptedException {
        long permits = cm.numBytes();
        if (permits > capacityLimit) {
            return MoreCompletableFutures.immediatelyFailed(new CapacityExceededException(permits, capacityLimit));
        }
        long remaining = obeyRateLimit(permits, timeout, unit);
        if (remaining <= 0) {
            return MoreCompletableFutures.immediatelyFailed(SUBMISSION_TIMEOUT);
        }
        capacityLimiter.tryAcquire(permits, remaining, unit);
        return doSubmit(cm, permits);
    }

    @Override
    public CompletionStage<Result> trySubmit(ConditionalMutation cm) {
        long permits = cm.numBytes();
        if (permits > capacityLimit) {
            return MoreCompletableFutures.immediatelyFailed(new CapacityExceededException(permits, capacityLimit));
        }
        boolean allowed = tryObeyRateLimit(permits);
        if (!allowed) {
            return MoreCompletableFutures.immediatelyFailed(SUBMISSION_TIMEOUT);
        }
        capacityLimiter.tryAcquire(permits);
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
        if (permits > capacityLimit) {
            return MoreCompletableFutures.immediatelyFailed(new CapacityExceededException(permits, capacityLimit));
        }
        obeyRateLimit(permits);
        capacityLimiter.acquire(permits);
        return doSubmitMany(mutations, permits);
    }

    @Override
    public CompletionStage<Collection<Result>> submitMany(Collection<ConditionalMutation> mutations, long timeout, TimeUnit unit) throws InterruptedException {
        long permits = countPermits(mutations);
        if (permits > capacityLimit) {
            return MoreCompletableFutures.immediatelyFailed(new CapacityExceededException(permits, capacityLimit));
        }
        long remaining = obeyRateLimit(permits, timeout, unit);
        if (remaining <= 0) {
            return MoreCompletableFutures.immediatelyFailed(SUBMISSION_TIMEOUT);
        }
        capacityLimiter.acquire(permits);
        return doSubmitMany(mutations, permits);
    }

    @Override
    public CompletionStage<Collection<Result>> trySubmitMany(Collection<ConditionalMutation> mutations) {
        long permits = countPermits(mutations);
        if (permits > capacityLimit) {
            return MoreCompletableFutures.immediatelyFailed(new CapacityExceededException(permits, capacityLimit));
        }
        boolean allowed = tryObeyRateLimit(permits);
        if (!allowed) {
            return MoreCompletableFutures.immediatelyFailed(SUBMISSION_TIMEOUT);
        }
        capacityLimiter.tryAcquire(permits);
        return doSubmitMany(mutations, permits);
    }

    private CompletionStage<Collection<Result>> doSubmitMany(Collection<ConditionalMutation> mutations, long permits) {
        Iterator<Result> resultIter = getResultIteratorOrReleasePermits(mutations, permits);
        CompleteManyTask task = new CompleteManyTask(resultIter, mutations.size(), permits);
        completionExecutor.execute(task);
        barrier.submit(task);
        return task;
    }

    private void obeyRateLimit(long permits) {
        if (rateLimiter != null) {
            if (permits > Integer.MAX_VALUE) {
                // in practice this should never happen and would
                // probably break Accumulo
                rateLimiter.acquire(Integer.MAX_VALUE);
            } else {
                rateLimiter.acquire((int) permits);
            }
        }
    }

    private long obeyRateLimit(long permits, long timeout, TimeUnit unit) {
        if (rateLimiter != null) {
            long start = System.nanoTime();
            boolean acquired = limitRate(permits, timeout, unit);
            if (!acquired) {
                return 0L;
            }
            return getRemainingInCorrectUnits(timeout, unit, start);
        }
        return timeout;
    }

    private long getRemainingInCorrectUnits(long timeout, TimeUnit unit, long start) {
        long remaining = timeout - (System.nanoTime() - start);
        return unit.convert(remaining, TimeUnit.NANOSECONDS);
    }

    private boolean limitRate(long permits, long timeout, TimeUnit unit) {
        if (permits > Integer.MAX_VALUE) {
            // in practice this should never happen and would probably break Accumulo
            return rateLimiter.tryAcquire(Integer.MAX_VALUE, timeout, unit);
        } else {
            return rateLimiter.tryAcquire((int) permits, timeout, unit);
        }
    }

    private boolean tryObeyRateLimit(long permits) {
        if (rateLimiter != null) {
            if (permits > Integer.MAX_VALUE) {
                // in practice this should never happen and would probably break Accumulo
                return rateLimiter.tryAcquire(Integer.MAX_VALUE);
            }
            return rateLimiter.tryAcquire((int) permits);
        }
        return true;
    }

    private Iterator<Result> getResultIteratorOrReleasePermits(ConditionalMutation cm, long permits) {
        return getResultIteratorOrReleasePermits(Collections.singleton(cm), permits);
    }

    private Iterator<Result> getResultIteratorOrReleasePermits(Collection<ConditionalMutation> mutations, long permits) {
        try {
            return writer.write(mutations.iterator());
        } catch (Throwable e) {
            capacityLimiter.release(permits);
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

    private ExecutorService defaultExecutor() {
        ThreadFactory tf = new ThreadFactoryBuilder()
                .setNameFormat("async-cw-" + id + "-%d")
                .build();

        // use an unbounded queue because the semaphore protects against OOME and we want
        // to avoid blocking when submitting tasks for trySubmit methods
        return new ThreadPoolExecutor(0, 16, 60L,
                TimeUnit.SECONDS, new LinkedBlockingQueue<>(), tf, new AbortPolicy());
    }

    private final class CompleteOneTask extends CompletableFuture<Result> implements Runnable {

        private Iterator<Result> resultIter;
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
            }
        }

        private Result getResult() {
            try {
                return resultIter.hasNext() ? resultIter.next() : null;
            } finally {
                resultIter = null;
                // release permits prior to calling complete in case the caller
                // schedules dependent completion actions to run in this thread,
                // particularly those that submit more mutations to this writer
                capacityLimiter.release(permits);
            }
        }

    }

    private final class CompleteManyTask extends CompletableFuture<Collection<Result>> implements Runnable {

        private Iterator<ConditionalWriter.Result> resultIter;
        private final int count;
        private final long permits;

        CompleteManyTask(Iterator<ConditionalWriter.Result> resultIter, int count, long permits) {
            this.resultIter = resultIter;
            this.count = count;
            this.permits = permits;
        }

        public void run() {
            try {
                complete(getResults());
            } catch (Throwable e) {
                completeExceptionally(e);
            }
        }

        private List<ConditionalWriter.Result> getResults() {
            try {
                return collectResults();
            } finally {
                resultIter = null;
                // release permits prior to calling complete in case the caller
                // schedules dependent completion actions to run in this thread,
                // particularly those that submit more mutations to this writer
                capacityLimiter.release(permits);
            }
        }

        private List<Result> collectResults() {
            List<Result> results = new ArrayList<>(count);
            while (resultIter.hasNext()) {
                results.add(resultIter.next());
            }
            return Collections.unmodifiableList(results);
        }

    }

}
