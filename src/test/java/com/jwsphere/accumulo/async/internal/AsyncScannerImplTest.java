package com.jwsphere.accumulo.async.internal;

import com.jwsphere.accumulo.async.AsyncScanner;
import com.jwsphere.accumulo.async.BlockingScanner;
import com.jwsphere.accumulo.async.Cell;
import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.concurrent.ForkJoinPool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AsyncScannerImplTest {

    @Test
    public void testBlockingScanner() {
        AsyncScanner scanner = AsyncSortedSetScanner.of(
                Cell.withEmptyValue(new Key("a")),
                Cell.withEmptyValue(new Key("b"))
        );

        BlockingScanner bs = new BlockingScanner(scanner);

        Iterator<Cell> iter = bs.iterator();
        assertTrue(iter.hasNext());
        assertEquals(Cell.withEmptyValue(new Key("a")), iter.next());
        assertTrue(iter.hasNext());
        assertEquals(Cell.withEmptyValue(new Key("b")), iter.next());
        assertFalse(iter.hasNext());
    }

}
