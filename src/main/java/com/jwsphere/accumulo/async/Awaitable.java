package com.jwsphere.accumulo.async;

import java.util.concurrent.TimeUnit;

/**
 * Represents a operation that may or may not be complete and provides
 * a way to wait for the operation to complete.
 *
 * @author Jonathan Wonders
 */
public interface Awaitable {

    /**
     * Waits until the condition associated with the barrier is satisfied.
     *
     * @throws InterruptedException If the calling thread is interrupted while waiting.
     */
    void await() throws InterruptedException;

    /**
     * Waits until the condition associated with the barrier is satisfied
     * or the timeout has elapsed.
     *
     * @return True if the condition was satisfied before the timeout elapsed, false otherwise.
     * @throws InterruptedException If the calling thread is interrupted while waiting.
     */
    boolean await(long timeout, TimeUnit unit) throws InterruptedException;

}
