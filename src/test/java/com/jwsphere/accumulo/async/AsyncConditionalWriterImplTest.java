package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.client.ConditionalWriter.Status;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import static com.jwsphere.accumulo.async.internal.Interruptible.propagateInterrupt;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(AccumuloParameterResolver.class)
public class AsyncConditionalWriterImplTest {

    private static AccumuloProvider accumulo;
    private static Connector connector;

    @BeforeAll
    public static void beforeAll(AccumuloProvider accumulo) throws Exception {
        AsyncConditionalWriterImplTest.accumulo = accumulo;
        ZooKeeperInstance instance = new ZooKeeperInstance(
                accumulo.getAccumuloCluster().getInstanceName(),
                accumulo.getAccumuloCluster().getZooKeepers()
        );
        connector = instance.getConnector(accumulo.getAdminUser(), accumulo.getAdminToken());
        connector.tableOperations().create("table");
    }

    @AfterAll
    public static void afterAll() throws Exception {
        connector.tableOperations().delete("table");
    }

    @Test
    public void testPutOne() throws Exception {
        AsyncConnector asyncConnector = new AsyncConnector(connector);
        AsyncConditionalWriterConfig config = AsyncConditionalWriterConfig.create();
        try (AsyncConditionalWriter writer = asyncConnector.createConditionalWriter("table", config)) {
            ConditionalMutation cm = new ConditionalMutation("put_one");
            cm.addCondition(new Condition("cf", "cq"));

            CompletionStage<Result> op = writer.submit(cm);

            Result result = op.toCompletableFuture().get();
            assertEquals(Status.ACCEPTED, result.getStatus());
        }
    }

    @Test
    public void testAwait() throws Exception {
        AsyncConnector asyncConnector = new AsyncConnector(connector);
        AsyncConditionalWriterConfig config = AsyncConditionalWriterConfig.create();
        try (AsyncConditionalWriter writer = asyncConnector.createConditionalWriter("table", config)) {
            ConditionalMutation cm = new ConditionalMutation("await");
            cm.addCondition(new Condition("cf", "cq"));

            CompletionStage<Result> op = writer.submit(cm);

            writer.await();

            assertTrue(op.toCompletableFuture().isDone());
            assertFalse(op.toCompletableFuture().isCompletedExceptionally());
        }
    }

    @Test
    public void putSeveralDependent() throws Exception {
        AsyncConnector asyncConnector = new AsyncConnector(connector);
        AsyncConditionalWriterConfig config = AsyncConditionalWriterConfig.create();
        try (AsyncConditionalWriter writer = asyncConnector.createConditionalWriter("table", config)) {

            ConditionalMutation cm1 = new ConditionalMutation("put_several_dependent");
            cm1.addCondition(new Condition("cf", "cq"));
            cm1.put("cf", "cq", "value");

            ConditionalMutation cm2 = new ConditionalMutation("put_several_dependent");
            cm2.addCondition(new Condition("cf", "cq").setValue("value"));
            cm2.putDelete("cf", "cq");

            CompletionStage<Result> op = writer.submit(cm1)
                    .thenCompose(propagateInterrupt(result -> writer.submit(cm2)))
                    .thenCompose(propagateInterrupt(result -> writer.submit(cm1)))
                    .thenCompose(propagateInterrupt(result -> writer.submit(cm2)));

            Result result = op.toCompletableFuture().get();
            assertEquals(Status.ACCEPTED, result.getStatus());
        }
    }

    @Test
    public void ensureExceedingCapacityFailsCleanly() throws Exception {

        AsyncConnector asyncConnector = new AsyncConnector(connector);
        AsyncConditionalWriterConfig config = AsyncConditionalWriterConfig.create()
                .withLimitedMemoryCapacity(1024);

        // large enough payload to exceed capacity
        byte[] payload = new byte[2048];

        ConditionalMutation cm = new ConditionalMutation("exceed_capacity");
        cm.addCondition(new Condition("cf", "cq"));
        cm.put("cf".getBytes(UTF_8), "cq".getBytes(UTF_8), payload);

        try (AsyncConditionalWriter writer = asyncConnector.createConditionalWriter("table", config)) {
            CompletionStage<Result> op = writer.submit(cm);
            assertThrows(ExecutionException.class, () -> op.toCompletableFuture().get());
        }

    }

    @Test
    public void ensureDependentMutationSubmissionsDoNotBlock() throws Exception {
        AsyncConnector asyncConnector = new AsyncConnector(connector);
        AsyncConditionalWriterConfig config = AsyncConditionalWriterConfig.create()
                .withLimitedMemoryCapacity(1024);

        try (AsyncConditionalWriter writer = asyncConnector.createConditionalWriter("table", config)) {

            // large enough payload so two mutations exceed capacity
            byte[] payload = new byte[768];

            ConditionalMutation cm1 = new ConditionalMutation("ensure_capacity_released");
            cm1.addCondition(new Condition("cf", "cq"));
            cm1.put("cf".getBytes(UTF_8), "cq".getBytes(UTF_8), payload);

            ConditionalMutation cm2 = new ConditionalMutation("ensure_capacity_released");
            cm2.addCondition(new Condition("cf2", "cq2"));
            cm2.put("cf2".getBytes(UTF_8), "cq2".getBytes(UTF_8), payload);

            CompletionStage<Result> op = writer.submit(cm1)
                    .thenCompose(propagateInterrupt(result -> writer.submit(cm2)));

            Result result = op.toCompletableFuture().get();
            assertEquals(Status.ACCEPTED, result.getStatus());
        }
    }

}
