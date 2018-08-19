package com.jwsphere.accumulo.async.internal;

import com.jwsphere.accumulo.async.AsyncScanner;
import com.jwsphere.accumulo.async.Cell;
import com.jwsphere.accumulo.async.ScanSubscriber;

import java.util.Arrays;
import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

public class AsyncSortedSetScanner implements AsyncScanner {

    private final SortedSet<Cell> cells;
    private final Executor executor;

    public AsyncSortedSetScanner(Collection<Cell> cells) {
        this.cells = new TreeSet<>(cells);
        this.executor = ForkJoinPool.commonPool();
    }

    public AsyncSortedSetScanner(Collection<Cell> cells, Executor executor) {
        this.cells = new TreeSet<>(cells);
        this.executor = executor;
    }

    @Override
    public void subscribe(ScanSubscriber observer) {
        executor.execute(() -> {
            ScanSubscriptionImpl controller = new ScanSubscriptionImpl();
            observer.onSubscribe(controller);
            for (Cell cell : cells) {
                controller.awaitFlowPermission();
                if (controller.isCancelled()) {
                    observer.onComplete();
                    return;
                }
                observer.onNext(cell);
            }
            observer.onComplete();
        });
    }

    public AsyncScanner withExecutor(Executor executor) {
        return new AsyncSortedSetScanner(cells, executor);
    }

    public static AsyncSortedSetScanner of(Cell... cells) {
        return new AsyncSortedSetScanner(Arrays.asList(cells));
    }

}
