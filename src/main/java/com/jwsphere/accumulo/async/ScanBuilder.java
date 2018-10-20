package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;

import java.util.concurrent.TimeUnit;

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

    ScanBuilder batchSize(int size);

    ScanBuilder batchTimeout(long timeout, TimeUnit unit);

    ScanBuilder readaheadThreshold(long batches);

    ScanBuilder timeout(long timeout, TimeUnit unit);

    AsyncScanner build();

}
