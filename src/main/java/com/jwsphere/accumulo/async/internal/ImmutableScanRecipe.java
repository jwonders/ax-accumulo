package com.jwsphere.accumulo.async.internal;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Collection;
import java.util.SortedSet;

public class ImmutableScanRecipe implements ScanRecipe {

    private final Range range;
    private final SortedSet<Column> fetchedColumns;
    private final Authorizations auth;
    private final boolean isolated;

    private final String classLoaderContext;
    private final Collection<IteratorSetting> iterators;
    private final SamplerConfiguration samplerConfig;

    private ImmutableScanRecipe(ScanRecipe recipe) {
        this.range = Ranges.deepCopyOf(recipe.getRange());
        this.fetchedColumns = Columns.deepCopyOf(recipe.getFetchedColumns());
        this.auth = recipe.getAuthorizations();
        this.isolated = recipe.isIsolated();
        this.classLoaderContext = recipe.getClassLoaderContext();
        this.iterators = recipe.getIterators(); // TODO deep copy
        this.samplerConfig = recipe.getSamplerConfiguration(); // TODO deep copy
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
        return iterators;
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
    public String getClassLoaderContext() {
        return classLoaderContext;
    }

    @Override
    public SamplerConfiguration getSamplerConfiguration() {
        return samplerConfig;
    }

    public static ImmutableScanRecipe copyOf(ScanRecipe recipe) {
        if (recipe instanceof ImmutableScanRecipe) {
            return (ImmutableScanRecipe) recipe;
        }
        return new ImmutableScanRecipe(recipe);
    }

}
