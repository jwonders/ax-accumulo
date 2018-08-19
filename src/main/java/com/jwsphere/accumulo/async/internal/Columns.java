package com.jwsphere.accumulo.async.internal;

import org.apache.accumulo.core.data.Column;

import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.Arrays.copyOf;

public class Columns {

    public static SortedSet<Column> deepCopyOf(SortedSet<Column> columns) {
        return columns.stream()
                .map(Columns::deepCopyOf)
                .collect(Collectors.toCollection(TreeSet::new));
    }

    public static Column deepCopyOf(Column column) {
        byte[] cf = column.getColumnFamily();
        byte[] cq = column.getColumnQualifier();
        byte[] cv = column.getColumnVisibility();
        return new Column(copyOf(cf, cf.length), copyOf(cq, cq.length), copyOf(cv, cv.length));
    }

}
