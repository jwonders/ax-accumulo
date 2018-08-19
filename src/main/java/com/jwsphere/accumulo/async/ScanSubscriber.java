package com.jwsphere.accumulo.async;

public interface ScanSubscriber /* extends Flow.Subscriber<Cell> JDK-9 */{

    default void onSubscribe(ScanSubscription subscription) {
        // simple case, no flow control
        subscription.request(Long.MAX_VALUE);
    }

    /**
     * Called with the next key-value pair of the scan.
     */
    default void onNext(Cell cell) {
    }

    // NOTE: Not quite sure about all the circumstances in which this would be called
    //       - unavailable tablet servers
    //       - unassigned tablets
    //       - table not found
    //       - table deleted
    //       - isolation exception

    /**
     * Called upon exceptional termination of the scan.  After {@code onError}
     * is called, there will be no more calls to {@code onNext}.
     */
    default void onError(Throwable throwable) {

    }

    /**
     * Called upon completion or cancellation of the scan.  Indicates that no more
     * calls will be made to <code>ScanSubscriber#onNext</code>
     */
    default void onComplete() {

    }

}
