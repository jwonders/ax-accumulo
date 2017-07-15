/*
 * This implementation was derived from a source file with the
 * following notice.
 *
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
package com.jwsphere.accumulo.async.internal;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedLongSynchronizer;

/**
 * A counting semaphore capable of maintaining a large set of permits.
 *
 * The semaphore behaves similarly to {@link java.util.concurrent.Semaphore}
 * but without support for fairness.  The primary use case for a high capacity
 * semaphore is to control access to high capacity resources such as
 * bytes of memory.
 *
 * @author Jonathan Wonders
 */
public class LongSemaphore {

    private final Synchronizer sync;

    static final class Synchronizer extends AbstractQueuedLongSynchronizer {

        Synchronizer(long permits) {
            setState(permits);
        }

        long getPermits() {
            return getState();
        }

        protected boolean tryReleaseShared(long permitsToRelease) {
            while (true) {
                long permitsAvailable = getState();
                long permitsAfterRelease = permitsAvailable + permitsToRelease;
                if (permitsAfterRelease < permitsAvailable) { // overflow
                    throw new IllegalStateException("Too many permits released. Exceeded permit limit.");
                }
                if (compareAndSetState(permitsAvailable, permitsAfterRelease)) {
                    return true;
                }
            }
        }

        protected long tryAcquireShared(long permitsToAcquire) {
            while (true) {
                long permitsAvailable = getState();
                long permitsRemaining = permitsAvailable - permitsToAcquire;
                if (permitsRemaining < 0 || compareAndSetState(permitsAvailable, permitsRemaining)) {
                    return permitsRemaining;
                }
            }
        }
    }

    public LongSemaphore(long permits) {
        sync = new Synchronizer(permits);
    }

    public void acquire() throws InterruptedException {
        sync.acquireSharedInterruptibly(1);
    }

    public void acquireUninterruptibly() {
        sync.acquireShared(1);
    }

    public boolean tryAcquire() {
        return sync.tryAcquireShared(1) >= 0;
    }

    public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        return sync.tryAcquireSharedNanos(1, unit.toNanos(timeout));
    }

    public void release() {
        sync.releaseShared(1);
    }

    public void acquire(long permits) throws InterruptedException {
        if (permits < 0) {
            throw new IllegalArgumentException();
        }
        sync.acquireSharedInterruptibly(permits);
    }

    public void acquireUninterruptibly(long permits) {
        if (permits < 0) {
            throw new IllegalArgumentException();
        }
        sync.acquireShared(permits);
    }

    public boolean tryAcquire(long permits) {
        if (permits < 0) {
            throw new IllegalArgumentException();
        }
        return sync.tryAcquireShared(permits) >= 0;
    }

    public boolean tryAcquire(long permits, long timeout, TimeUnit unit) throws InterruptedException {
        if (permits < 0) {
            throw new IllegalArgumentException();
        }
        return sync.tryAcquireSharedNanos(permits, unit.toNanos(timeout));
    }

    public void release(long permits) {
        if (permits < 0) {
            throw new IllegalArgumentException();
        }
        sync.releaseShared(permits);
    }

    public long availablePermits() {
        return sync.getPermits();
    }

    public String toString() {
        return super.toString() + sync.getPermits();
    }

}
