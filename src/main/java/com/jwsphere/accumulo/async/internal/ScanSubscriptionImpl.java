package com.jwsphere.accumulo.async.internal;

import com.jwsphere.accumulo.async.ScanSubscription;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ScanSubscriptionImpl implements ScanSubscription {

    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final Lock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();

    // @GuardedBy("lock")
    private long requested = 0;

    @Override
    public void request(long elements) {
        lock.lock();
        try {
            requested = addWithoutOverflow(requested, elements);
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void cancel() {
        cancelled.set(true);
    }

    boolean isCancelled() {
        return cancelled.get();
    }

    public void awaitFlowPermission() {
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

    private long addWithoutOverflow(long left, long right) {
        long sum = left + right;
        if (((left ^ sum) & (right ^ sum)) < 0L) {
            return Long.MAX_VALUE;
        } else {
            return sum;
        }
    }

}
