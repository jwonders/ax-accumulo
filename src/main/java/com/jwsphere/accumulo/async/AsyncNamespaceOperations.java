package com.jwsphere.accumulo.async;

import com.jwsphere.accumulo.async.internal.Checked;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.NamespaceOperations;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.tukaani.xz.check.Check;

import java.util.EnumSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

public class AsyncNamespaceOperations {

    private final NamespaceOperations namespaceOps;
    private final Executor executor;

    public AsyncNamespaceOperations(NamespaceOperations namespaceOps, Executor executor) {
        this.namespaceOps = namespaceOps;
        this.executor = executor;
    }

    public CompletionStage<SortedSet<String>> list() {
        return Checked.supplyAsync(namespaceOps::list, executor);
    }

    public CompletionStage<Boolean> exists(String namespace) {
        return Checked.supplyAsync(() -> namespaceOps.exists(namespace), executor);
    }

    public CompletionStage<Void> create(String namespace) {
        return Checked.runAsync(() -> namespaceOps.create(namespace), executor);
    }

    public CompletionStage<Void> delete(String namespace) {
        return Checked.runAsync(() -> namespaceOps.delete(namespace), executor);
    }

    public CompletionStage<Void> rename(String oldNamespaceName, String newNamespaceName) {
        return Checked.runAsync(() -> namespaceOps.rename(oldNamespaceName, newNamespaceName), executor);
    }

    public CompletionStage<Void> setProperty(String namespace, String property, String value) {
        return Checked.runAsync(() -> namespaceOps.setProperty(namespace, property, value), executor);
    }

    public CompletionStage<Void> removeProperty(String namespace, String property) {
        return Checked.runAsync(() -> namespaceOps.removeProperty(namespace, property), executor);
    }

    public CompletionStage<Iterable<Entry<String, String>>> getProperties(String namespace) {
        return Checked.supplyAsync(() -> namespaceOps.getProperties(namespace), executor);
    }

    public CompletionStage<Map<String, String>> namespaceIdMap() {
        return Checked.supplyAsync(namespaceOps::namespaceIdMap, executor);
    }

    public CompletionStage<Void> attachIterator(String namespace, IteratorSetting setting) {
        return Checked.runAsync(() -> namespaceOps.attachIterator(namespace, setting), executor);
    }

    public CompletionStage<Void> attachIterator(String namespace, IteratorSetting setting, EnumSet<IteratorScope> scopes) {
        return Checked.runAsync(() -> namespaceOps.attachIterator(namespace, setting, scopes), executor);
    }

    public CompletionStage<Void> removeIterator(String namespace, String name, EnumSet<IteratorScope> scopes) {
        return Checked.runAsync(() -> namespaceOps.removeIterator(namespace, name, scopes), executor);
    }

    public CompletionStage<IteratorSetting> getIteratorSetting(String namespace, String name, IteratorScope scope) {
        return Checked.supplyAsync(() -> namespaceOps.getIteratorSetting(namespace, name, scope), executor);
    }

    public CompletionStage<Map<String, EnumSet<IteratorScope>>> listIterators(String namespace) {
        return Checked.supplyAsync(() -> namespaceOps.listIterators(namespace), executor);
    }

    public CompletionStage<Void> checkIteratorConflicts(String namespace, IteratorSetting setting, EnumSet<IteratorScope> scopes) {
        return Checked.runAsync(() -> namespaceOps.checkIteratorConflicts(namespace, setting, scopes), executor);
    }

    public CompletionStage<Integer> addConstraint(String namespace, String constraintClassName) {
        return Checked.supplyAsync(() -> namespaceOps.addConstraint(namespace, constraintClassName), executor);
    }

    public CompletionStage<Void> removeConstraint(String namespace, int id) {
        return Checked.runAsync(() -> namespaceOps.removeConstraint(namespace, id), executor);
    }

    public CompletionStage<Map<String, Integer>> listConstraints(String namespace) {
        return Checked.supplyAsync(() -> namespaceOps.listConstraints(namespace), executor);
    }

    public CompletionStage<Boolean> testClassLoad(String namespace, String className, String asTypeName) {
        return Checked.supplyAsync(() -> namespaceOps.testClassLoad(namespace, className, asTypeName), executor);
    }

}
