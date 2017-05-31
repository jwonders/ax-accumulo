package com.jwsphere.accumulo.async.internal;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.function.Supplier;

public class Managed {

    public static Runnable runnable(Runnable runnable) {
        return new ManagedRunnable(runnable);
    }

    public static <T> Supplier<T> supplier(Supplier<T> supplier) {
        return new ManagedSupplier<>(supplier);
    }

    public static final class ManagedSupplier<T> implements Supplier<T> {

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

        private final Supplier<T> supplier;
        private T result;
        private boolean done;

        SupplierManagedBlocker(Supplier<T> supplier) {
            this.supplier = supplier;
            this.done = false;
        }

        @Override
        public boolean block() throws InterruptedException {
            result = supplier.get();
            done = true;
            return true;
        }

        @Override
        public boolean isReleasable() {
            return done;
        }

        public T getResult() {
            return result;
        }

    }

    private static final class RunnableManagedBlocker implements ForkJoinPool.ManagedBlocker {

        private final Runnable runnable;
        private boolean done;

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
