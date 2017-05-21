package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.*;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

public class AsyncTableOperations {

    private final TableOperations tableOps;
    private final ExecutorService executor;

    public AsyncTableOperations(TableOperations operations) {
        this(operations, ForkJoinPool.commonPool());
    }

    public AsyncTableOperations(TableOperations operations, ExecutorService executor) {
        this.tableOps = operations;
        this.executor = executor;
    }

    /**
     * Initiates the retrieval of a list of tables in Accumulo.
     *
     * @return A completion stage representing listing of tables.
     */
    public CompletionStage<SortedSet<String>> list() {
        return supplyAsync(tableOps::list);
    }

    public CompletionStage<Boolean> exists(String tableName) {
        return supplyAsync(() -> tableOps.exists(tableName));
    }

    /**
     * Initiates the creation of a table, returning immediately. Creates a table
     * with no special configuration
     *
     * @param tableName
     *          the name of the table
     */
    public CompletionStage<Void> create(String tableName) {
        return runAsync(() -> tableOps.create(tableName));
    }

    /**
     * Initiates the creation of a table with the given configuration, returning immediately.
     *
     * @param tableName
     *          the name of the table
     * @param config
     *          specifies the new table's configuration variable, which are: 1. enable/disable the
     *          versioning iterator, which will limit the number of Key versions kept; 2. specifies
     *          logical or real-time based time recording for entries in the table; 3. user defined
     *          properties to be merged into the initial properties of the table
     */
    public CompletionStage<Void> create(String tableName, NewTableConfiguration config) {
        return runAsync(() -> tableOps.create(tableName, config));
    }

    /**
     * Ensures that tablets are split along a set of keys.
     *
     * @see TableOperations#addSplits(String, SortedSet)
     *
     * @param tableName
     *          the name of the table
     * @param partitionKeys
     *          a sorted set of row key values to pre-split the table on
     */
    public CompletionStage<Void> addSplits(String tableName, SortedSet<Text> partitionKeys) {
        return runAsync(() -> tableOps.addSplits(tableName, partitionKeys));
    }

    public CompletionStage<Collection<Text>> listSplits(String tableName) {
        return supplyAsync(() -> tableOps.listSplits(tableName));
    }

    public CompletionStage<Collection<Text>> listSplits(String tableName, int maxSplits) {
        return supplyAsync(() -> tableOps.listSplits(tableName, maxSplits));
    }

    public CompletionStage<Locations> locate(String tableName, Collection<Range> ranges) {
        return supplyAsync(() -> tableOps.locate(tableName, ranges));
    }

    public CompletionStage<Text> getMaxRow(String tableName, Authorizations auths, Text startRow, boolean startInclusive, Text endRow, boolean endInclusive) {
        return supplyAsync(() -> tableOps.getMaxRow(tableName, auths, startRow, startInclusive, endRow, endInclusive));
    }

    public CompletionStage<Void> merge(String tableName, Text start, Text end) {
        return runAsync(() -> tableOps.merge(tableName, start, end));
    }

    public CompletionStage<Void> deleteRows(String tableName, Text start, Text end) {
        return runAsync(() -> tableOps.deleteRows(tableName, start, end));
    }

    public CompletionStage<Void> compact(String tableName, Text start, Text end, boolean flush) {
        return runAsync(() -> tableOps.compact(tableName, start, end, flush, true));
    }

    public CompletionStage<Void> compact(String tableName, Text start, Text end, List<IteratorSetting> iterators, boolean flush) {
        return runAsync(() -> tableOps.compact(tableName, start, end, iterators, flush, true));
    }

    public CompletionStage<Void> compact(String tableName, CompactionConfig config) {
        return runAsync(() -> tableOps.compact(tableName, config));
    }

    public CompletionStage<Void> cancelCompaction(String tableName) {
        return runAsync(() -> tableOps.cancelCompaction(tableName));
    }

    public CompletionStage<Void> delete(String tableName) {
        return runAsync(() -> tableOps.delete(tableName));
    }

    public CompletionStage<Void> deleteMany(Iterable<String> tableNames) {
        CompletableFuture[] futures = StreamSupport.stream(tableNames.spliterator(), false)
                .map(this::delete)
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures);
    }

    public CompletionStage<Void> deleteMany(String... tableNames) {
        return CompletableFuture.allOf(Arrays.stream(tableNames).map(this::delete).toArray(CompletableFuture[]::new));
    }

    public CompletionStage<Void> rename(String oldTableName, String newTableName) {
        return runAsync(() -> tableOps.rename(oldTableName, newTableName));
    }

    public CompletionStage<Void> flush(String tableName, Text start, Text end) {
        return runAsync(() -> tableOps.flush(tableName, start, end, true));
    }

    public CompletionStage<Iterable<Map.Entry<String, String>>> getProperties(String tableName) {
        return supplyAsync(() -> tableOps.getProperties(tableName));
    }

    public CompletionStage<Void> setLocalityGroups(String tableName, Map<String, Set<Text>> groups) {
        return runAsync(() -> tableOps.setLocalityGroups(tableName, groups));
    }

    public CompletionStage<Map<String, Set<Text>>> getLocalityGroups(String tableName) {
        return supplyAsync(() -> tableOps.getLocalityGroups(tableName));
    }

    public CompletionStage<Set<Range>> splitRangeByTablets(String tableName, Range range, int maxSplits) {
        return supplyAsync(() -> tableOps.splitRangeByTablets(tableName, range, maxSplits));
    }

    public CompletionStage<Void> importDirectory(String tableName, String dir, String failureDir, boolean setTime) {
        return runAsync(() -> tableOps.importDirectory(tableName, dir, failureDir, setTime));
    }

    public CompletionStage<Void> offline(String tableName) {
        return runAsync(() -> tableOps.offline(tableName, true));
    }

    public CompletionStage<Void> online(String tableName) {
        return runAsync(() -> tableOps.online(tableName, true));
    }

    public CompletionStage<Void> attachIterator(String tableName, IteratorSetting iterator) {
        return runAsync(() -> tableOps.attachIterator(tableName, iterator));
    }

    public CompletionStage<Void> attachIterator(String tableName, IteratorSetting iterator, EnumSet<IteratorUtil.IteratorScope> scopes) {
        return runAsync(() -> tableOps.attachIterator(tableName, iterator, scopes));
    }

    public CompletionStage<Void> removeIterator(String tableName, String name, EnumSet<IteratorUtil.IteratorScope> scopes) {
        return runAsync(() -> tableOps.removeIterator(tableName, name, scopes));
    }

    public CompletionStage<IteratorSetting> getIteratorSetting(String tableName, String name, IteratorUtil.IteratorScope scope) {
        return supplyAsync(() -> tableOps.getIteratorSetting(tableName, name, scope));
    }

    public CompletionStage<Map<String, EnumSet<IteratorUtil.IteratorScope>>> listIterators(String tableName) {
        return supplyAsync(() -> tableOps.listIterators(tableName));
    }

    public CompletionStage<Void> checkIteratorConflicts(String tableName, IteratorSetting setting, EnumSet<IteratorUtil.IteratorScope> scopes) {
        return runAsync(() -> tableOps.checkIteratorConflicts(tableName, setting, scopes));
    }

    public CompletionStage<Integer> addConstraint(String tableName, String constraintClassName) {
        return supplyAsync(() -> tableOps.addConstraint(tableName, constraintClassName));
    }

    public CompletionStage<Void> removeConstraint(String tableName, int constraint) {
        return runAsync(() -> tableOps.removeConstraint(tableName, constraint));
    }

    public CompletionStage<Map<String, Integer>> listConstraints(String tableName) {
        return supplyAsync(() -> tableOps.listConstraints(tableName));
    }

    public CompletionStage<List<DiskUsage>> getDiskUsage(Set<String> tables) {
        return supplyAsync(() -> tableOps.getDiskUsage(tables));
    }

    public CompletionStage<Boolean> testClassLoad(String tableName, String className, String asTypeName) {
        return supplyAsync(() -> tableOps.testClassLoad(tableName, className, asTypeName));
    }

    public CompletionStage<Void> setSamplerConfiguration(String tableName, SamplerConfiguration config) {
        return runAsync(() -> tableOps.setSamplerConfiguration(tableName, config));
    }

    public CompletionStage<Void> clearSamplerConfiguration(String tableName) {
        return runAsync(() -> tableOps.clearSamplerConfiguration(tableName));
    }

    public CompletionStage<SamplerConfiguration> getSamplerConfiguration(String tableName) {
        return supplyAsync(() -> tableOps.getSamplerConfiguration(tableName));
    }

    private CompletionStage<Void> runAsync(CheckedRunnable runnable) {
        return CompletableFuture.runAsync(propagate(runnable), executor);
    }

    private <T> CompletionStage<T> supplyAsync(CheckedSupplier<T> supplier) {
        return CompletableFuture.supplyAsync(propagate(supplier), executor);
    }

    private static Runnable propagate(CheckedRunnable runnable) {
        return () -> {
            try {
                runnable.run();
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        };
    }

    private static <T> Supplier<T> propagate(CheckedSupplier<T> supplier) {
        return () -> {
            try {
                return supplier.get();
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        };
    }

    @FunctionalInterface
    private interface CheckedRunnable {
        void run() throws Exception;
    }

    @FunctionalInterface
    private interface CheckedSupplier<T> {
        T get() throws Exception;
    }
}
