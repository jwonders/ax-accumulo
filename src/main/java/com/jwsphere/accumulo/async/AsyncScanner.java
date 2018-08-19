package com.jwsphere.accumulo.async;

/**
 * An async scanner behaves as an observable source of key-value pairs
 * resulting from a scan.
 */
public interface AsyncScanner /* extends Flow.Publisher JDK-9 */{

    void subscribe(ScanSubscriber observer);

}
