package com.jwsphere.accumulo.async.internal;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

public class Unchecked {

    public static Runnable runnable(CheckedRunnable runnable) {
        return () -> {
            try {
                runnable.run();
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        };
    }

    public static <T> Supplier<T> supplier(CheckedSupplier<T> supplier) {
        return () -> {
            try {
                return supplier.get();
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        };
    }

    public static CompletionStage<Void> runAsync(CheckedRunnable runnable, Executor executor) {
        return CompletableFuture.runAsync(Unchecked.runnable(runnable), executor);
    }

    public static <T> CompletionStage<T> supplyAsync(CheckedSupplier<T> supplier, Executor executor) {
        return CompletableFuture.supplyAsync(Unchecked.supplier(supplier), executor);
    }

    @FunctionalInterface
    public interface CheckedRunnable {
        void run() throws Exception;
    }

    @FunctionalInterface
    public interface CheckedSupplier<T> {
        T get() throws Exception;
    }

    @FunctionalInterface
    public interface CheckedFunction<T, R> {
        R apply(T arg) throws Exception;
    }
}
