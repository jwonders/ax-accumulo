package com.jwsphere.accumulo.async;

public interface ScanSubscription /* extends Flow.Subscription JDK-9 */ {

    /**
     * <p>
     * Permits the scan executor to call {@link ScanSubscriber#onNext(Cell)}
     * up to {@code elements} times.  This allows the observer to control flow of
     * items from the scanner to the observer thereby applying back-pressure to
     * the underlying data source. This method may be called from any thread,
     * however it is common to request additional element(s) during a call to
     * {@link ScanSubscriber#onNext(Cell)}. Other common strategies involve
     * deferring the request for additional elements until some amount of previous
     * elements can be reclaimed by the garbage collector.
     * </p>
     *
     * <p>
     * The <code>ScanSubscriber</code> is responsible for requesting items until
     * completion or requesting cancellation.  By default, implementations of
     * <code>ScanSubscriber</code> will request <code>Long.MAX_VALUE</code> items
     * to effectively allow uninhibited flow.
     * </p>
     */
    void request(long elements);

    /**
     * Requests cancellation of the scan.  The {@code cancel} method may be called
     * safely from any thread.  Cancellation requests must be detected between
     * successive calls to {@link ScanSubscriber#onNext(Cell)}.
     *
     * If cancellation is requested within the body of {@code onNext}, the observer
     * can expect that no more calls will be made to {@code onNext}.  However, if
     * cancellation is requested from a different thread, the observer may see one
     * additional call to {@code onNext}.
     *
     * A cancellation request does not guarantee no additional work will be
     * performed, but in practice can prevent a scan form being fully evaluated.
     */
    void cancel();

}
