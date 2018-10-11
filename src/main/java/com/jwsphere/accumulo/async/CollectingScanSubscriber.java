package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * A scan subscribed that collects observed results.
 */
public class CollectingScanSubscriber<R> extends CompletableFuture<R> implements ScanSubscriber {

    private final Helper<?, R> helper;

    private CollectingScanSubscriber(Collector<Cell, ?, R> collector) {
        this.helper = new Helper<>(collector);
    }

    @Override
    public void onNext(Cell cell) {
        helper.collect(cell);
    }

    @Override
    public void onError(Throwable t) {
        completeExceptionally(t);
    }

    @Override
    public void onComplete() {
        complete(helper.result());
    }

    public static <T> CollectingScanSubscriber<T> collect(Collector<Cell, ?, T> collector) {
        return new CollectingScanSubscriber<>(collector);
    }

    public static <T extends Collection<Cell>> CollectingScanSubscriber<T> toCollection(Supplier<T> supplier) {
        return new CollectingScanSubscriber<>(Collectors.toCollection(supplier));
    }

    public static CollectingScanSubscriber<List<Cell>> toList() {
        return toCollection(ArrayList::new);
    }

    public static CollectingScanSubscriber<SortedSet<Cell>> toSortedSet() {
        return toCollection(TreeSet::new);
    }

    public static CollectingScanSubscriber<Map<Key, Value>> toMap() {
        return new CollectingScanSubscriber<>(Collectors.toMap(Cell::getKey, Cell::getValue));
    }

    public static <K, V> CollectingScanSubscriber<Map<K, V>> toMap(
            Function<? super Cell, K> toKey, Function<? super Cell, V> toValue) {
        return new CollectingScanSubscriber<>(Collectors.toMap(toKey, toValue));
    }

    public static <K, V, M extends Map<K, V>> CollectingScanSubscriber<M> toMap(
            Function<? super Cell, K> toKey,
            Function<? super Cell, V> toValue,
            Supplier<M> mapSupplier) {
        return new CollectingScanSubscriber<>(Collectors.toMap(toKey, toValue, throwingMerger(), mapSupplier));
    }

    public static <K, V> CollectingScanSubscriber<Map<K, V>> toMap(
            Function<? super Cell, K> toKey,
            Function<? super Cell, V> toValue,
            BinaryOperator<V> mergeStrategy) {
        return new CollectingScanSubscriber<>(Collectors.toMap(toKey, toValue, mergeStrategy));
    }

    public static <K, V, M extends Map<K, V>> CollectingScanSubscriber<M> toMap(
            Function<? super Cell, K> toKey,
            Function<? super Cell, V> toValue,
            BinaryOperator<V> mergeStrategy,
            Supplier<M> mapSupplier) {
        return new CollectingScanSubscriber<>(Collectors.toMap(toKey, toValue, mergeStrategy, mapSupplier));
    }

    public static CollectingScanSubscriber<SortedMap<Key, Value>> toSortedMap() {
        return new CollectingScanSubscriber<>(Collectors.toMap(Cell::getKey, Cell::getValue, throwingMerger(), TreeMap::new));
    }

    public static <K extends Comparable<K>, V> CollectingScanSubscriber<SortedMap<K, V>> toSortedMap(
            Function<? super Cell, K> toKey,
            Function<? super Cell, V> toValue) {
        return new CollectingScanSubscriber<>(Collectors.toMap(toKey, toValue, throwingMerger(), TreeMap::new));
    }

    public static <K, V> CollectingScanSubscriber<SortedMap<K, V>> toSortedMap(
            Function<? super Cell, K> toKey,
            Function<? super Cell, V> toValue,
            BinaryOperator<V> mergeStrategy) {
        return new CollectingScanSubscriber<>(Collectors.toMap(toKey, toValue, mergeStrategy, TreeMap::new));
    }

    private static class Helper<A, C> {

        private final Collector<Cell, A, C> collector;
        private final A accumulated;

        public Helper(Collector<Cell, A, C> collector) {
            this.collector = collector;
            this.accumulated = collector.supplier().get();
        }

        public void collect(Cell cell) {
            collector.accumulator().accept(accumulated, cell);
        }

        public C result() {
            return collector.finisher().apply(accumulated);
        }

    }

    private static <T> BinaryOperator<T> throwingMerger() {
        return (key, value) -> {
            throw new IllegalStateException(String.format("Duplicate key %s", key));
        };
    }

}
