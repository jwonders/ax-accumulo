package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public interface ScanController {

    /**
     * Requests the scan executor to call {@link ScanObserver#onNext(Key, Value)}
     * up to {@code elements} times.  Provides a mechanism for flow-control to the scan observer.
     */
    // NOTE: this must probably be thread safe to allow the observer to make requests
    //       asynchronously when the latest message has been processed
    //       Probably not too difficult to achieve with an atomic counter.  Just need
    //       to be careful about decrementing upon calling onNext(...)
    void request(int elements);

    /**
     * Requests cancellation
     */
    // NOTE: This probably must be called from within the context of a call to onNext(...)
    //       Otherwise, there may be confusion about whether the call to cancel happens-before
    //       a call to onNext(...).  If a caller wants to trigger cancellation from another
    //       thread, it could be done via a volatile that is observed sometime within the
    //       context of a call to onNext(...).
    void cancel();

}
