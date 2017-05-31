package com.jwsphere.accumulo.async;

import java.util.concurrent.TimeUnit;

public interface Barrier {

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
