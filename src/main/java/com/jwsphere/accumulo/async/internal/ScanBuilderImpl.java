package com.jwsphere.accumulo.async.internal;

import com.jwsphere.accumulo.async.AsyncScanner;
import com.jwsphere.accumulo.async.ScanBuilder;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class ScanBuilderImpl implements ScanBuilder {

    private final Connector connector;
    private final String tableName;
    private final MutableScanRecipe recipe;
    private final Executor executor;

    public ScanBuilderImpl(Connector connector, String tableName, Executor executor) {
        this.connector = connector;
        this.tableName = tableName;
        this.recipe = new MutableScanRecipe();
        this.executor = executor;
    }

    @Override
    public ScanBuilder range(Range range) {
        recipe.setRange(range);
        return this;
    }

    @Override
    public ScanBuilder fetchColumnFamily(byte[] cf) {
        Objects.requireNonNull(cf, "Column family must be non-null.");
        recipe.addFetchedColumnFamily(cf);
        return this;
    }

    @Override
    public ScanBuilder fetchColumn(byte[] cf, byte[] cq) {
        Objects.requireNonNull(cf, "Column family must be non-null.");
        recipe.addFetchedColumn(cf, cq);
        return this;
    }

    @Override
    public ScanBuilder iterator(IteratorSetting settings) {
        recipe.addIterator(settings);
        return this;
    }

    @Override
    public ScanBuilder isolation(boolean isolated) {
        recipe.setIsolated(isolated);
        return this;
    }

    @Override
    public ScanBuilder authorizations(Authorizations auth) {
        recipe.setAuthorizations(auth);
        return this;
    }

    @Override
    public ScanBuilder batchSize(int size) {
        recipe.setBatchSize(size);
        return this;
    }

    @Override
    public ScanBuilder batchTimeout(long timeout, TimeUnit unit) {
        recipe.setBatchTimeout(timeout, unit);
        return null;
    }

    @Override
    public ScanBuilder readaheadThreshold(long batches) {
        recipe.setReadaheadThreshold(batches);
        return null;
    }

    @Override
    public ScanBuilder timeout(long timeout, TimeUnit unit) {
        recipe.setTimeout(timeout, unit);
        return null;
    }

    @Override
    public ScanBuilder samplerConfiguration(SamplerConfiguration samplerConfig) {
        recipe.setSamplerConfiguration(samplerConfig);
        return this;
    }

    @Override
    public ScanBuilder classLoaderContext(String classLoaderContext) {
        recipe.setClassLoaderContext(classLoaderContext);
        return this;
    }

    @Override
    public AsyncScanner build() {
        return new AsyncScannerImpl(connector, tableName, recipe, executor);
    }

}
