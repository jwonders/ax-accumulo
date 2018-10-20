package com.jwsphere.accumulo.async.internal;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.stream.Collectors;

public class ImmutableScanRecipe implements ScanRecipe {

    private final Range range;
    private final SortedSet<Column> fetchedColumns;
    private final Authorizations auth;
    private final boolean isolated;
    private final int batchSize;
    private final long batchTimeout;
    private final long readaheadThreshold;
    private final long timeout;

    private final String classLoaderContext;
    private final Collection<IteratorSetting> iterators;
    private final SamplerConfiguration samplerConfig;

    private ImmutableScanRecipe(ScanRecipe recipe) {
        this.range = Ranges.deepCopyOf(recipe.getRange());
        this.fetchedColumns = Columns.deepCopyOf(recipe.getFetchedColumns());
        this.auth = recipe.getAuthorizations();
        this.isolated = recipe.isIsolated();
        this.classLoaderContext = recipe.getClassLoaderContext();
        this.batchSize = recipe.getBatchSize();
        this.readaheadThreshold = recipe.getReadaheadThreshold();
        this.batchTimeout = recipe.getBatchTimeout();
        this.timeout = recipe.getTimeout();
        this.iterators = copyOf(recipe.getIterators());
        this.samplerConfig = copyOf(recipe.getSamplerConfiguration());
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
        return copyOf(iterators);
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
        return copyOf(samplerConfig);
    }

    public static ImmutableScanRecipe copyOf(ScanRecipe recipe) {
        if (recipe instanceof ImmutableScanRecipe) {
            return (ImmutableScanRecipe) recipe;
        }
        return new ImmutableScanRecipe(recipe);
    }

    private static List<IteratorSetting> copyOf(Collection<IteratorSetting> iterators) {
        return iterators.stream().map(ImmutableScanRecipe::copyOf).collect(Collectors.toList());
    }

    private static IteratorSetting copyOf(IteratorSetting setting) {
        IteratorSetting copy = new IteratorSetting(setting.getPriority(), setting.getName(), setting.getIteratorClass());
        copy.addOptions(setting.getOptions());
        return copy;
    }

    private static SamplerConfiguration copyOf(SamplerConfiguration setting) {
        if (setting == null) {
            return null;
        }
        SamplerConfiguration copy = new SamplerConfiguration(setting.getSamplerClassName());
        copy.setOptions(setting.getOptions());
        return copy;
    }

}
