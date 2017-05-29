package com.jwsphere.accumulo.async.internal;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

public class Checked {

    public static CompletionStage<Void> runAsync(CheckedRunnable runnable, Executor executor) {
        return CompletableFuture.runAsync(propagate(runnable), executor);
    }

    public static <T> CompletionStage<T> supplyAsync(CheckedSupplier<T> supplier, Executor executor) {
        return CompletableFuture.supplyAsync(propagate(supplier), executor);
    }

    public static Runnable propagate(CheckedRunnable runnable) {
        return () -> {
            try {
                runnable.run();
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        };
    }

    public static <T> Supplier<T> propagate(CheckedSupplier<T> supplier) {
        return () -> {
            try {
                return supplier.get();
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        };
    }

    @FunctionalInterface
    public interface CheckedRunnable {
        void run() throws Exception;
    }

    @FunctionalInterface
    public interface CheckedSupplier<T> {
        T get() throws Exception;
    }

}
