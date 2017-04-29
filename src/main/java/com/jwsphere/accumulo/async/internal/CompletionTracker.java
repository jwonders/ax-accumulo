package com.jwsphere.accumulo.async.internal;

import javax.annotation.concurrent.GuardedBy;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * Tracks groups of asynchronous operations, allowing callers to wait until
 * previously submitted operations have completed.  A completion action is
 * associated with each operation to inform the tracker of completion.
 *
 * Each operation is associated with a generation number upon submission and
 * this generation number is advanced upon each wait request.
 */
public class CompletionTracker {

    private static final BiFunction<Long, Long, Long> GENERATION_INCOMPLETE_INCREMENTER =
            (generation, incomplete) -> incomplete == null ? 1 : incomplete + 1;

    private static final BiFunction<Long, Long, Long> GENERATION_INCOMPLETE_DECREMENTER =
            (generation, incomplete) -> incomplete - 1;

    /**
     * The current generation.
     */
    private long generation = 0L;

    /**
     * Upon completion, an operation will run this task which notifies the tracker
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
     * Tracks the completion of a completable future with respect to threads waiting
     * on previously-submitted futures.
     */
    public synchronized <T> CompletionStage<T> submit(final CompletionStage<T> stage) {
        numIncompleteByGeneration.compute(generation, GENERATION_INCOMPLETE_INCREMENTER);
        return stage.toCompletableFuture().whenComplete(currentGeneration);
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

    /**
     * Waits until previously submitted operations have completed.
     *
     * @throws InterruptedException If this thread is interrupted while waiting.
     */
    public synchronized void await() throws InterruptedException {
        final long gen = generation;
        generation++;
        currentGeneration = new CompletionTask(generation);

        boolean complete = false;
        while (!complete) {
            complete = countNumIncomplete(numIncompleteByGeneration.subMap(0L, gen + 1)) == 0;
            if (!complete) {
                this.wait();
            }
        }
    }

    private long countNumIncomplete(Map<Long, Long> needToWaitFor) {
        return needToWaitFor.values().stream().mapToLong(Long::longValue).sum();
    }

    private final class CompletionTask implements BiConsumer<Object, Throwable> {

        private final long generation;

        // must be construted while lock is held
        CompletionTask(long generation) {
            this.generation = generation;
        }

        @Override
        public void accept(Object o, Throwable throwable) {
            complete(generation);
        }

    }

}
