package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;

public interface ScanBuilder {

    ScanBuilder range(Range range);

    // Can these be replaced with iterators now ???
    ScanBuilder fetchColumnFamily(byte[] cf);

    // Can these be replaced with iterators now ???
    ScanBuilder fetchColumn(byte[] cf, byte[] cq);

    ScanBuilder iterator(IteratorSetting settings);

    ScanBuilder isolation(boolean isolated);

    ScanBuilder authorizations(Authorizations auth);

    ScanBuilder samplerConfiguration(SamplerConfiguration config);

    ScanBuilder classLoaderContext(String classLoaderContext);

    AsyncScanner build();

    /*
     * Timeouts could possibly be handled through cancellation rather than
     * being an up-front configuration.
     */
    //@Deprecated
    //ScanBuilder timeout(long timeout, TimeUnit unit);

    /*
     * The notion of batches could go away depending on the underlying
     * async implementation.
     */
    //@Deprecated
    //ScanBuilder batchSize(int size);

    /*
     * The notion of batches could go away depending on the underlying
     * async implementation.
     */
    //@Deprecated
    //ScanBuilder batchTimeout(long timeout, TimeUnit unit);

    /*
     * Read-ahead is an implementation detail of the current scanner and
     * might not make sense with an end-to-end async implementation
     */
    //@Deprecated
    //ScanBuilder readaheadThreshold(long batches);

}
