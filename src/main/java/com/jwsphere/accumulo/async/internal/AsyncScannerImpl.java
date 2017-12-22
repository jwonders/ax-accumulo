package com.jwsphere.accumulo.async.internal;

import com.jwsphere.accumulo.async.AsyncScanner;
import com.jwsphere.accumulo.async.ScanController;
import com.jwsphere.accumulo.async.ScanObserver;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An "async" scanner that simply iterates through scan results and
 * calls the observer when results are available.
 */
public class AsyncScannerImpl implements AsyncScanner {

    private final Connector connector;
    private final String tableName;
    private final ScanRecipe recipe;

    AsyncScannerImpl(Connector connector, String tableName, ScanRecipe recipe) {
        this.connector = connector;
        this.tableName = tableName;
        this.recipe = ImmutableScanRecipe.copyOf(recipe);
    }

    @Override
    public void scan(ScanObserver observer) {
        try {
            Scanner scanner = createScanner();
            ScanControllerImpl controller = new ScanControllerImpl();
            observer.onInitialize(controller);
            for (Entry<Key, Value> next : scanner) {
                controller.respectFlowControl();
                boolean cancelled = controller.cancelled.get();
                if (cancelled) {
                    observer.onComplete();
                    return;
                }
                observer.onNext(next.getKey(), next.getValue());
            }
            observer.onComplete();

        } catch (TableNotFoundException | RuntimeException e) {
            observer.onError(e);
        }
    }

    @Override
    public void scanOn(ScanObserver observer, Executor executor) {
        executor.execute(() -> scan(observer));
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

    private static final class ScanControllerImpl implements ScanController {


        private final AtomicBoolean cancelled = new AtomicBoolean(false);
        private final Lock lock = new ReentrantLock();
        private final Condition notEmpty = lock.newCondition();

        // @GuardedBy("lock")
        private long requested = 0;

        @Override
        public void request(int elements) {
            lock.lock();
            try {
                requested += elements;
                notEmpty.signalAll();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void cancel() {
            cancelled.set(true);
        }

        private void respectFlowControl() {
            lock.lock();
            try {
                while (requested <= 0) {
                    notEmpty.await();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }
        }

    }

}
