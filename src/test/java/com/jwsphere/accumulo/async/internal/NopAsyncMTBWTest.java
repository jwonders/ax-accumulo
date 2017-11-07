package com.jwsphere.accumulo.async.internal;

import com.jwsphere.accumulo.async.AsyncBatchWriter;
import com.jwsphere.accumulo.async.AsyncMultiTableBatchWriter;
import org.apache.accumulo.core.data.Mutation;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class NopAsyncMTBWTest {

    @Test
    @Disabled
    public void testCachedBatchWriterPutMany() throws Exception {
        byte[] value = new byte[256];

        try (AsyncMultiTableBatchWriter bw = new AsyncMultiTableBatchWriterImpl(NopMultiTableBatchWriter::new)) {
            AsyncBatchWriter abw = bw.getBatchWriter("table");

            AtomicLong submitted = new AtomicLong();
            AtomicLong count = new AtomicLong();

            long start = System.currentTimeMillis();

            while (true) {
                Random random = ThreadLocalRandom.current();
                byte[] row = ByteBuffer.allocate(8).putLong(random.nextLong()).array();
                byte[] cq = ByteBuffer.allocate(8).putLong(random.nextLong()).array();
                Mutation m = new Mutation(row);
                m.put("cf".getBytes(StandardCharsets.UTF_8), cq, value);
                abw.submit(m, 1, TimeUnit.SECONDS).thenRun(count::incrementAndGet);
                long s = submitted.incrementAndGet();

                if (s > 0 && s % 100_000 == 0) {
                    System.out.println(submitted.get() + " ==> " + count.get() + " - " + (System.currentTimeMillis() - start));
                }
            }

        }
    }

}
