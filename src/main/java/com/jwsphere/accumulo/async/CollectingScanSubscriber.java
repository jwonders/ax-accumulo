package com.jwsphere.accumulo.async;

import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;

public class CollectingScanSubscriber extends CompletableFuture<SortedSet<Cell>> implements ScanSubscriber {

    private final SortedSet<Cell> entries = new TreeSet<>();

    @Override
    public void onNext(Cell cell) {
        entries.add(cell);
    }

    @Override
    public void onError(Throwable t) {
        completeExceptionally(t);
    }

    @Override
    public void onComplete() {
        complete(entries);
    }

}
