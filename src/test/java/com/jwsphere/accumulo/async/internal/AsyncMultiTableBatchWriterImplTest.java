package com.jwsphere.accumulo.async.internal;

import com.jwsphere.accumulo.async.AccumuloParameterResolver;
import com.jwsphere.accumulo.async.AccumuloProvider;
import com.jwsphere.accumulo.async.AsyncBatchWriter;
import com.jwsphere.accumulo.async.AsyncConnector;
import com.jwsphere.accumulo.async.AsyncMultiTableBatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Mutation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(AccumuloParameterResolver.class)
public class AsyncMultiTableBatchWriterImplTest {

    private static AccumuloProvider accumulo;
    private static Connector connector;

    @BeforeAll
    public static void beforeAll(AccumuloProvider accumulo) throws Exception {
        AsyncMultiTableBatchWriterImplTest.accumulo = accumulo;
        connector = accumulo.newAdminConnector();
        connector.tableOperations().create("table");
    }

    @AfterAll
    public static void afterAll() throws Exception {
        connector.tableOperations().delete("table");
    }

    @Test
    public void testPutOne() throws Exception {
        AsyncConnector asyncConnector = AsyncConnector.wrap(connector);
        BatchWriterConfig config = new BatchWriterConfig();
        try (AsyncMultiTableBatchWriter bw = asyncConnector.createMultiTableBatchWriter(config)) {
            Mutation m = new Mutation("put one");
            m.put("cf", "cq", "value");
            CompletableFuture<Void> op = bw.submit("table", m).toCompletableFuture();
            op.get();
            assertTrue(op.isDone());
            assertFalse(op.isCompletedExceptionally());
        }
    }

    @Test
    public void testCachedBatchWriterPutOne() throws Exception {
        AsyncConnector asyncConnector = AsyncConnector.wrap(connector);
        BatchWriterConfig config = new BatchWriterConfig();
        try (AsyncMultiTableBatchWriter bw = asyncConnector.createMultiTableBatchWriter(config)) {
            AsyncBatchWriter abw = bw.getBatchWriter("table");
            Mutation m = new Mutation("put one cached bw");
            m.put("cf", "cq", "value");
            CompletableFuture<Void> op = abw.submit(m).toCompletableFuture();
            op.get();
            assertTrue(op.isDone());
            assertFalse(op.isCompletedExceptionally());
        }
    }

    @Test
    public void testAwait() throws Exception {
        AsyncConnector asyncConnector = AsyncConnector.wrap(connector);
        BatchWriterConfig config = new BatchWriterConfig();
        try (AsyncMultiTableBatchWriter bw = asyncConnector.createMultiTableBatchWriter(config)) {
            Mutation m = new Mutation("await");
            m.put("cf", "cq", "value");
            CompletableFuture<Void> op = bw.submit("table", m).toCompletableFuture();
            bw.await();
            assertTrue(op.isDone());
            assertFalse(op.isCompletedExceptionally());
        }
    }

}
