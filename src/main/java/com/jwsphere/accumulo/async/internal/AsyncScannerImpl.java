package com.jwsphere.accumulo.async.internal;

import com.jwsphere.accumulo.async.AsyncScanner;
import com.jwsphere.accumulo.async.Cell;
import com.jwsphere.accumulo.async.ScanSubscriber;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.util.Map.Entry;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

/**
 * An "async" scanner that simply iterates through scan results and
 * calls the observer when results are available.  The iteration may
 * scheduled on a thread other than the calling thread.
 */
public class AsyncScannerImpl implements AsyncScanner {

    private final Connector connector;
    private final String tableName;
    private final ScanRecipe recipe;
    private final Executor executor;

    public AsyncScannerImpl(Connector connector, String tableName, ScanRecipe recipe) {
        this(connector, tableName, recipe, ForkJoinPool.commonPool());
    }

    public AsyncScannerImpl(Connector connector, String tableName, ScanRecipe recipe, Executor executor) {
        this.connector = connector;
        this.tableName = tableName;
        this.recipe = ImmutableScanRecipe.copyOf(recipe);
        this.executor = executor;
    }

    @Override
    public void subscribe(ScanSubscriber observer) {
        executor.execute(scanTask(observer));
    }

    private Runnable scanTask(ScanSubscriber observer) {
        return () -> {
            // since the scanner blocks on calls to hasNext, it doesn't really
            // make sense to try and multiplex the execution of multiple scans
            // on a single thread
            try (Scanner scanner = createScanner()) {
                ScanSubscriptionImpl controller = new ScanSubscriptionImpl();
                observer.onSubscribe(controller);
                for (Entry<Key, Value> next : scanner) {
                    controller.awaitFlowPermission();
                    if (controller.isCancelled()) {
                        observer.onComplete();
                        return;
                    }
                    observer.onNext(Cell.of(next.getKey(), next.getValue()));
                }
                observer.onComplete();
            } catch (TableNotFoundException | RuntimeException e) {
                observer.onError(e);
            }
        };
    }

    private Scanner createScanner() throws TableNotFoundException {
        Scanner scanner = this.connector.createScanner(tableName, recipe.getAuthorizations());
        scanner.setRange(recipe.getRange());

        for (Column fetched : recipe.getFetchedColumns()) {
            scanner.fetchColumn(new IteratorSetting.Column(new Text(fetched.getColumnFamily()), new Text(fetched.getColumnQualifier())));
        }

        if (recipe.isIsolated()) {
            scanner.enableIsolation();
        } else {
            scanner.disableIsolation();
        }

        for (IteratorSetting iteratorSetting : recipe.getIterators()) {
            scanner.addScanIterator(iteratorSetting);
        }
        return scanner;
    }

}
