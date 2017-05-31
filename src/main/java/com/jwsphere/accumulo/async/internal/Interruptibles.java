package com.jwsphere.accumulo.async.internal;

import com.jwsphere.accumulo.async.internal.Checked.CheckedFunction;
import com.jwsphere.accumulo.async.internal.Checked.CheckedRunnable;
import com.jwsphere.accumulo.async.internal.Checked.CheckedSupplier;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

public class Interruptibles {

    @FunctionalInterface
    public interface InterruptibleRunnable extends CheckedRunnable {
        void run() throws InterruptedException;
    }

    @FunctionalInterface
    public interface InterruptibleSupplier<T> extends CheckedSupplier<T> {
        T get() throws InterruptedException;
    }

    @FunctionalInterface
    public interface InterruptibleFunction<T, R> extends CheckedFunction<T, R> {
        R apply(T arg);
    }

    @FunctionalInterface
    public interface InterruptionHandler extends Function<InterruptedException, RuntimeException> {
        RuntimeException apply(InterruptedException e);
    }

    public static Runnable runnable(InterruptibleRunnable runnable) {
        return runnable(runnable, CompletionException::new);
    }

    public static Runnable runnable(InterruptibleRunnable runnable, InterruptionHandler handler) {
        return () -> {
            try {
                runnable.run();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw handler.apply(e);
            }
        };
    }

    public static <T> Supplier<T> supplier(InterruptibleSupplier<T> supplier) {
        return supplier(supplier, CompletionException::new);
    }

    public static <T> Supplier<T> supplier(InterruptibleSupplier<T> supplier, InterruptionHandler handler) {
        return () -> {
            try {
                return supplier.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw handler.apply(e);
            }
        };
    }

    public static <T> CompletionStage<T> failOnInterrupt(InterruptibleSupplier<CompletionStage<T>> supplier) {
        try {
            return supplier.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            CompletableFuture<T> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }
    }

}
