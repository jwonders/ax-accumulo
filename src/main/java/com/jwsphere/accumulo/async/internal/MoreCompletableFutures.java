package com.jwsphere.accumulo.async.internal;

import com.jwsphere.accumulo.async.internal.Unchecked.CheckedRunnable;
import com.jwsphere.accumulo.async.internal.Unchecked.CheckedSupplier;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

public class MoreCompletableFutures {

    public static CompletableFuture<Void> runAsync(CheckedRunnable runnable, Executor executor) {
        if (executor instanceof ForkJoinPool) {
            // if we are running in a fork join pool, use a managed blocker
            return CompletableFuture.runAsync(Managed.runnable(Unchecked.runnable(runnable)));
        }
        return CompletableFuture.runAsync(Unchecked.runnable(runnable), executor);
    }

    public static <T> CompletableFuture<T> supplyAsync(CheckedSupplier<T> supplier, Executor executor) {
        if (executor instanceof ForkJoinPool) {
            // if we are running in a fork join pool, use a managed blocker
            return CompletableFuture.supplyAsync(Managed.supplier(Unchecked.supplier(supplier)));
        }
        return CompletableFuture.supplyAsync(Unchecked.supplier(supplier), executor);
    }

    public static <T> CompletableFuture<T> immediatelyFailed(Throwable t) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(t);
        return future;
    }

}
