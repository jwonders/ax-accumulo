package com.jwsphere.accumulo.async.internal;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Collection;
import java.util.SortedSet;

public interface ScanRecipe {

    Range getRange();

    Authorizations getAuthorizations();

    Collection<IteratorSetting> getIterators();

    SortedSet<Column> getFetchedColumns();

    boolean isIsolated();

    String getClassLoaderContext();

    SamplerConfiguration getSamplerConfiguration();

}
