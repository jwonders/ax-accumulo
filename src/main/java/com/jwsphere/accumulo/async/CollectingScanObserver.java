package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.util.AbstractMap;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;

public class CollectingScanObserver extends CompletableFuture<SortedSet<Entry<Key, Value>>> implements ScanObserver {

    private final SortedSet<Entry<Key, Value>> entries = new TreeSet<>(Entry.comparingByKey());

    @Override
    public void onNext(Key key, Value value) {
        entries.add(new AbstractMap.SimpleImmutableEntry<>(key, value));
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
