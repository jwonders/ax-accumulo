package com.jwsphere.accumulo.async.internal;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;

import java.util.Arrays;

public class Ranges {

    public static Range deepCopyOf(Range range) {
        Key startKey = deepCopyOf(range.getStartKey());
        Key endKey = deepCopyOf(range.getEndKey());
        return new Range(
                startKey, endKey,
                range.isStartKeyInclusive(), range.isEndKeyInclusive(),
                startKey == null, endKey == null
        );
    }

    private static Key deepCopyOf(Key key) {
        if (key == null) {
            return null;
        }

        final byte[] row = copyToArray(key.getRowData());
        final byte[] cf = copyToArray(key.getColumnFamilyData());
        final byte[] cq = copyToArray(key.getColumnQualifierData());
        final byte[] cv = copyToArray(key.getColumnVisibilityData());
        long timestamp = key.getTimestamp();

        return new Key(
                row, 0, row.length,
                cf, 0, cf.length,
                cq, 0, cq.length,
                cv, 0, cv.length,
                timestamp
        );
    }

    private static byte[] copyToArray(ByteSequence row) {
        final byte[] rowCopy;
        if (row.isBackedByArray()) {
            rowCopy = Arrays.copyOfRange(row.getBackingArray(), row.offset(), row.length());
        } else {
            rowCopy = row.toArray();
        }
        return rowCopy;
    }

}
