package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collector;

/**
 * An async scanner behaves as an observable source of key-value pairs
 * resulting from a scan.
 */
public interface AsyncScanner /* extends Flow.Publisher JDK-9 */{

    void subscribe(ScanSubscriber observer);

    default <T> CompletableFuture<T> collect(Collector<Cell, ?, T> collector) {
        return CollectingScanSubscriber.collect(collector);
    }

    default CompletableFuture<List<Cell>> toList() {
        CollectingScanSubscriber<List<Cell>> subscriber = CollectingScanSubscriber.toList();
        subscribe(subscriber);
        return subscriber;
    }

    default CompletableFuture<SortedSet<Cell>> toSortedSet() {
        CollectingScanSubscriber<SortedSet<Cell>> subscriber = CollectingScanSubscriber.toSortedSet();
        subscribe(subscriber);
        return subscriber;
    }

    default CompletableFuture<SortedMap<Key, Value>> toSortedMap() {
        CollectingScanSubscriber<SortedMap<Key, Value>> subscriber =
                CollectingScanSubscriber.toSortedMap(Cell::getKey, Cell::getValue);
        subscribe(subscriber);
        return subscriber;
    }

}
