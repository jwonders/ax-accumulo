package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public interface ScanObserver {

    // NOTE: Not quite sure about all the circumstances in which this would be called
    //       - unavailable tablet servers
    //       - unassigned tablets
    //       - table not found
    //       - table deleted
    default void onError(Throwable t) {
        // is there a way to continue after errors in some cases
    }

    default void onInitialize(ScanController controller) {
        // simple case, no flow control
        controller.request(Integer.MAX_VALUE);
    }

    /**
     * Called with the next key-value pair of the scan.
     */
    default void onNext(Key key, Value value) {
    }

    default void onComplete() {
    }

}
