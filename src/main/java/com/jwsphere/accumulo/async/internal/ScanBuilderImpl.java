package com.jwsphere.accumulo.async.internal;

import com.jwsphere.accumulo.async.AsyncScanner;
import com.jwsphere.accumulo.async.ScanBuilder;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Objects;

public class ScanBuilderImpl implements ScanBuilder {

    private final Connector connector;
    private final String tableName;
    private final MutableScanRecipe recipe;

    public ScanBuilderImpl(Connector connector, String tableName) {
        this.connector = connector;
        this.tableName = tableName;
        this.recipe = new MutableScanRecipe();
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
        return new AsyncScannerImpl(connector, tableName, recipe);
    }

}
