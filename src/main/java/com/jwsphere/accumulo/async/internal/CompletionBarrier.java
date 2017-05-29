package com.jwsphere.accumulo.async.internal;

import javax.annotation.concurrent.GuardedBy;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * Tracks groups of asynchronous operations, allowing callers to wait until
 * previously submitted operations have completed.  A completion action is
 * associated with each operation to inform the barrier of completion.
 *
 * Each operation is associated with a generation number upon submission and
 * this generation number is advanced upon each wait request.
 */
public class CompletionBarrier {

    private static final BiFunction<Long, Long, Long> GENERATION_INCOMPLETE_INCREMENTER =
            (generation, incomplete) -> incomplete == null ? 1 : incomplete + 1;

    private static final BiFunction<Long, Long, Long> GENERATION_INCOMPLETE_DECREMENTER =
            (generation, incomplete) -> incomplete - 1;

    /**
     * The current generation.
     */
    @GuardedBy("this")
    private long generation = 0L;

    /**
     * Upon completion, an operation will run this task which notifies the barrier
     * of its completion.  One task can be shared between all operations belonging
     * to a generation.  This task is replaced when the generation is advanced.
     */
    @GuardedBy("this")
    private CompletionTask currentGeneration = new CompletionTask(generation);

    /**
     * Keeps record of the number of incomplete tasks for each generation.
     */
    @GuardedBy("this")
    private final SortedMap<Long, Long> numIncompleteByGeneration = new TreeMap<>();

    /**
     * Tracks the completion of a completion stage.  If a call to submit happens-before
     * a call to await, the given completion stage will be complete when the call to
     * await returns.
     */
    public synchronized <T> CompletionStage<T> submit(final CompletionStage<T> stage) {
        if (stage instanceof CompletableFuture) {
            CompletableFuture<T> future = stage.toCompletableFuture();
            // no need to track if already completed
            if (future.isDone()) {
                return future;
            }
        }
        numIncompleteByGeneration.compute(generation, GENERATION_INCOMPLETE_INCREMENTER);
        return stage.whenComplete(currentGeneration);
    }

    /**
     * Tracks the completion of a completion stage.  If a call to submit happens-before
     * a call to await, the given completion stage will be complete when the call to
     * await returns.
     *
     * A completion action will be scheduled for asynchronous execution on the default
     * executor.  This action will notify the barrier of completion.
     */
    public synchronized <T> CompletionStage<T> submitAsync(final CompletionStage<T> stage) {
        if (stage instanceof CompletableFuture) {
            CompletableFuture<T> future = stage.toCompletableFuture();
            // no need to track if already completed
            if (future.isDone()) {
                return future;
            }
        }
        numIncompleteByGeneration.compute(generation, GENERATION_INCOMPLETE_INCREMENTER);
        return stage.whenCompleteAsync(currentGeneration);
    }

    /**
     * Tracks the completion of a completion stage.  If a call to submit happens-before
     * a call to await, the given completion stage will be complete when the call to
     * await returns.
     *
     * A completion action will be scheduled for asynchronous execution on the provided
     * executor.  This action will notify the barrier of completion.
     */
    public synchronized <T> CompletionStage<T> submitAsync(final CompletionStage<T> stage, Executor executor) {
        if (stage instanceof CompletableFuture) {
            CompletableFuture<T> future = stage.toCompletableFuture();
            // no need to track if already completed
            if (future.isDone()) {
                return future;
            }
        }
        numIncompleteByGeneration.compute(generation, GENERATION_INCOMPLETE_INCREMENTER);
        return stage.whenCompleteAsync(currentGeneration, executor);
    }

    /**
     * Counts the number of incomplete operations.  This method may be used to monitor
     * the number of operations that would need to complete if a call were made to
     * {@link #await()} instead.  This method should not be called in a tight loop as
     * it may prevent operations from completing in a timely manner.
     *
     * @return The current number of incomplete operations.
     */
    public synchronized long incomplete() {
        return countNumIncomplete(completionsToWaitFor(generation));
    }

    /**
     * Waits indefinitely until previously submitted operations have completed.
     *
     * @throws InterruptedException If this thread is interrupted while waiting.
     */
    public synchronized void await() throws InterruptedException {
        final long gen = generation;
        generation++;
        currentGeneration = new CompletionTask(generation);
        waitUntilComplete(gen);
    }

    private void waitUntilComplete(long generation) throws InterruptedException {
        boolean complete = false;
        while (!complete) {
            complete = countNumIncomplete(completionsToWaitFor(generation)) == 0;
            if (!complete) {
                this.wait();
            }
        }
    }

    /**
     * Waits until previously submitted operations have completed or the timeout
     * has elapsed.
     *
     * @return true if all operations have completed, false otherwise
     * @throws InterruptedException If this thread is interrupted while waiting.
     */
    public synchronized boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        final long gen = generation;
        generation++;
        currentGeneration = new CompletionTask(generation);
        return waitUntilComplete(gen, timeout, unit);
    }

    private boolean waitUntilComplete(long generation, long timeout, TimeUnit unit) throws InterruptedException {
        long end = System.nanoTime() + unit.toNanos(timeout);
        boolean complete = false;
        while (!complete && System.nanoTime() < end) {
            complete = countNumIncomplete(completionsToWaitFor(generation)) == 0;
            if (!complete) {
                TimeUnit.NANOSECONDS.timedWait(this, end - System.nanoTime());
            }
        }
        return complete;
    }

    private SortedMap<Long, Long> completionsToWaitFor(long generation) {
        return numIncompleteByGeneration.subMap(0L, generation + 1);
    }

    private long countNumIncomplete(Map<Long, Long> needToWaitFor) {
        return needToWaitFor.values().stream().mapToLong(Long::longValue).sum();
    }

    /**
     * Completes one operation for the given generation.
     */
    private synchronized void complete(long generation) {
        long incomplete = numIncompleteByGeneration.compute(generation, GENERATION_INCOMPLETE_DECREMENTER);
        if (incomplete == 0) {
            numIncompleteByGeneration.remove(generation);
            this.notifyAll();
        }
    }

    private final class CompletionTask implements BiConsumer<Object, Throwable> {

        private final long generation;

        CompletionTask(long generation) {
            this.generation = generation;
        }

        @Override
        public void accept(Object o, Throwable throwable) {
            complete(generation);
        }

    }

}
