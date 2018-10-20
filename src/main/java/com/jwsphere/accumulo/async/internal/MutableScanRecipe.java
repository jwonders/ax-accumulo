package com.jwsphere.accumulo.async.internal;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

public class MutableScanRecipe implements ScanRecipe {

    private Range range = new Range();
    private SortedSet<Column> fetchedColumns = new TreeSet<>();
    private Authorizations auth = Authorizations.EMPTY;
    private boolean isolated = false;
    private int batchSize = 100;
    private long batchTimeout = Long.MAX_VALUE;
    private long readaheadThreshold = 1;
    private long timeout = Long.MAX_VALUE;

    private String classLoaderContext = null;
    private Map<String, IteratorSetting> iteratorSettingsByName = new HashMap<>();
    private SamplerConfiguration samplerConfig = null;

    public void setRange(Range range) {
        this.range = range;
    }

    public void setAuthorizations(Authorizations auth) {
        this.auth = auth;
    }

    public void addFetchedColumnFamily(byte[] cf) {
        fetchedColumns.add(new Column(cf, null, null));
    }

    public void addFetchedColumn(byte[] cf, byte[] cq) {
        fetchedColumns.add(new Column(cf, cq, null));
    }

    public void setIsolated(boolean isolated) {
        this.isolated = isolated;
    }

    public void addIterator(IteratorSetting iteratorSetting) {
        this.iteratorSettingsByName.put(iteratorSetting.getName(), iteratorSetting);
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void setReadaheadThreshold(long readaheadThreshold) {
        this.readaheadThreshold = readaheadThreshold;
    }

    public void setBatchTimeout(long batchTimeout, TimeUnit unit) {
        this.batchTimeout = unit.toMillis(batchTimeout);
    }

    public void setTimeout(long timeout, TimeUnit unit) {
        this.timeout = unit.toMillis(timeout);
    }

    public void setSamplerConfiguration(SamplerConfiguration samplerConfig) {
        this.samplerConfig = samplerConfig;
    }

    public void setClassLoaderContext(String classLoaderContext) {
        this.classLoaderContext = classLoaderContext;
    }

    @Override
    public Range getRange() {
        return range;
    }

    @Override
    public Authorizations getAuthorizations() {
        return auth;
    }

    @Override
    public Collection<IteratorSetting> getIterators() {
        return iteratorSettingsByName.values();
    }

    @Override
    public SortedSet<Column> getFetchedColumns() {
        return fetchedColumns;
    }

    @Override
    public boolean isIsolated() {
        return isolated;
    }

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public long getReadaheadThreshold() {
        return readaheadThreshold;
    }

    @Override
    public long getBatchTimeout() {
        return batchTimeout;
    }

    @Override
    public long getTimeout() {
        return timeout;
    }

    @Override
    public String getClassLoaderContext() {
        return classLoaderContext;
    }

    @Override
    public SamplerConfiguration getSamplerConfiguration() {
        return samplerConfig;
    }

}
