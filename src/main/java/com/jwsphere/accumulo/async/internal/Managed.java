package com.jwsphere.accumulo.async.internal;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.function.Supplier;

/**
 * This class contains utilities to support managed blocking of tasks
 * submitted to ForkJoinPool's.
 *
 * When submitting tasks that might block to a ForkJoinPool, especially
 * the common pool, it is important to perform the task in the context of a
 * managed block to inform the pool that it may need to create additional
 * threads if all are currently blocked.
 *
 * Submitting many small tasks that may block a thread for long periods of
 * time to a ForkJoinPool may lead to many threads being created.  It is
 * probably better to consider using a separate bounded executor or using
 * a cooperative mechanism in these cases.
 */
public class Managed {

    private Managed() {
        // hiding implicit default constructor
    }

    /**
     * Decorates the provided runnable so it performs a managed block.
     */
    public static Runnable runnable(Runnable runnable) {
        if (runnable instanceof ManagedRunnable) {
            return runnable;
        }
        return new ManagedRunnable(runnable);
    }

    /**
     * Decorates the provided supplier so it performs a managed block.
     */
    public static <T> Supplier<T> supplier(Supplier<T> supplier) {
        if (supplier instanceof ManagedSupplier) {
            return supplier;
        }
        return new ManagedSupplier<>(supplier);
    }

    private static final class ManagedSupplier<T> implements Supplier<T> {

        private final Supplier<T> supplier;

        ManagedSupplier(Supplier<T> supplier) {
            this.supplier = supplier;
        }

        @Override
        public T get() {
            try {
                SupplierManagedBlocker<T> blocker = new SupplierManagedBlocker<>(supplier);
                ForkJoinPool.managedBlock(blocker);
                return blocker.getResult();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CompletionException(e);
            }
        }

    }

    private static class ManagedRunnable extends ForkJoinTask<Void> implements Runnable {

        private final Runnable runnable;

        ManagedRunnable(Runnable runnable) {
            this.runnable = runnable;
        }

        @Override
        public Void getRawResult() {
            return null;
        }

        @Override
        protected void setRawResult(Void value) {
        }

        @Override
        protected boolean exec() {
            run();
            return true;
        }

        @Override
        public void run() {
            try {
                ForkJoinPool.managedBlock(new RunnableManagedBlocker(runnable));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CompletionException(e);
            }
        }

    }

    private static final class SupplierManagedBlocker<T> implements ForkJoinPool.ManagedBlocker {

        private static final Object NIL = new Object();

        private final Supplier<T> supplier;
        private volatile Object result;

        SupplierManagedBlocker(Supplier<T> supplier) {
            this.supplier = supplier;
            this.result = NIL;
        }

        @Override
        public boolean block() throws InterruptedException {
            result = supplier.get();
            return true;
        }

        @Override
        public boolean isReleasable() {
            return result != NIL;
        }

        @SuppressWarnings("unchecked")
        public T getResult() {
            if (result == NIL) {
                throw new IllegalStateException();
            }
            return (T) result;
        }

    }

    private static final class RunnableManagedBlocker implements ForkJoinPool.ManagedBlocker {

        private final Runnable runnable;
        private volatile boolean done;

        RunnableManagedBlocker(Runnable runnable) {
            this.runnable = runnable;
            this.done = false;
        }

        @Override
        public boolean block() throws InterruptedException {
            runnable.run();
            done = true;
            return true;
        }

        @Override
        public boolean isReleasable() {
            return done;
        }

    }

}
