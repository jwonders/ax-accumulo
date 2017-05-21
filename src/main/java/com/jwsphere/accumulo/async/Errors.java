package com.jwsphere.accumulo.async;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class Errors {

    @FunctionalInterface
    public interface InterruptibleSupplier<T> {
        T get() throws InterruptedException;
    }

    public static <T> CompletionStage<T> failOnInterrupt(InterruptibleSupplier<CompletionStage<T>> supplier) {
        try {
            return supplier.get();
        } catch (InterruptedException e) {
            CompletableFuture<T> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }
    }

}
