package com.jwsphere.accumulo.async.internal;

import com.jwsphere.accumulo.async.AccumuloParameterResolver;
import com.jwsphere.accumulo.async.AccumuloProvider;
import com.jwsphere.accumulo.async.AsyncConditionalWriter;
import com.jwsphere.accumulo.async.AsyncConditionalWriter.SingleWriteStage;
import com.jwsphere.accumulo.async.AsyncConditionalWriter.WriteStage;
import com.jwsphere.accumulo.async.AsyncConditionalWriterConfig;
import com.jwsphere.accumulo.async.AsyncConnector;
import com.jwsphere.accumulo.async.AsyncScanner;
import com.jwsphere.accumulo.async.FailurePolicy;
import com.jwsphere.accumulo.async.ResultException;
import com.jwsphere.accumulo.async.Results;
import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.client.ConditionalWriter.Status;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Range;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.jwsphere.accumulo.async.Results.getStatusUnchecked;
import static com.jwsphere.accumulo.async.internal.Interruptible.failOnInterrupt;
import static com.jwsphere.accumulo.async.internal.MoreCompletableFutures.immediatelyFailed;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.accumulo.core.client.ConditionalWriter.Status.UNKNOWN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(AccumuloParameterResolver.class)
public class AsyncConditionalWriterImplTest {

    private static Connector connector;
    private static AsyncConnector asyncConnector;

    @BeforeAll
    public static void beforeAll(AccumuloProvider accumulo) throws Exception {
        connector = accumulo.newAdminConnector();
        asyncConnector = AsyncConnector.wrap(connector);
        connector.tableOperations().create("table");
    }

    @AfterAll
    public static void afterAll() throws Exception {
        connector.tableOperations().delete("table");
    }

    @Test
    public void testPutOne() throws Exception {
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
    public void testRejectOne() throws Exception {
        AsyncConditionalWriterConfig config = AsyncConditionalWriterConfig.create();
        try (AsyncConditionalWriter writer = asyncConnector.createConditionalWriter("table", config)) {
            ConditionalMutation cm = new ConditionalMutation("reject one");
            cm.addCondition(new Condition("cf", "cq").setValue(new byte[0]));

            CompletionStage<Result> op = writer.submit(cm);

            Result result = op.toCompletableFuture().get();
            assertEquals(Status.REJECTED, result.getStatus());
        }
    }

    @Test
    public void testRejectFirst() throws Exception {
        AsyncConditionalWriterConfig config = AsyncConditionalWriterConfig.create();
        try (AsyncConditionalWriter writer = asyncConnector.createConditionalWriter("table", config)
                .withFailurePolicy(FailurePolicy.failUnlessAccepted())) {

            ConditionalMutation cm = new ConditionalMutation("put_one");
            cm.addCondition(new Condition("cf", "cq").setValue(new byte[0]));

            ConditionalMutation cm2 = new ConditionalMutation("put_one");
            cm2.addCondition(new Condition("cf", "cq"));

            WriteStage<Result> first = writer.submit(cm);
            WriteStage<Result> second = first.thenSubmit(cm2);

            assertThrows(Exception.class, first.toCompletableFuture()::get);
            assertThrows(Exception.class, second.toCompletableFuture()::get);
        }
    }

    @Test
    public void testPutOneWithTimeLimit() {
        assertThrows(CompletionException.class, () -> {
            AsyncConditionalWriterConfig config = AsyncConditionalWriterConfig.create();
            try (AsyncConditionalWriter writer = asyncConnector.createConditionalWriter("table", config).withRateLimit(1024)) {
                byte[] payload = new byte[8096];
                ConditionalMutation cm1 = new ConditionalMutation("put_one_consume_rate_permits");
                cm1.addCondition(new Condition("cf", "cq"));
                cm1.put("cf".getBytes(UTF_8), "cq".getBytes(UTF_8), payload);

                ConditionalMutation cm2 = new ConditionalMutation("put_one_exceed_time_limit");
                cm2.addCondition(new Condition("cf2", "cq2"));
                cm2.put("cf2".getBytes(UTF_8), "cq2".getBytes(UTF_8), payload);

                SingleWriteStage second = writer.submit(cm1).thenSubmit(cm2, 100, TimeUnit.MILLISECONDS);

                second.toCompletableFuture().join();
            }
        });
    }


    @Test
    public void testAwait() throws Exception {
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
        AsyncConditionalWriterConfig config = AsyncConditionalWriterConfig.create();
        try (AsyncConditionalWriter writer = asyncConnector.createConditionalWriter("table", config)) {

            ConditionalMutation cm1 = new ConditionalMutation("put_several_dependent");
            cm1.addCondition(new Condition("cf", "cq"));
            cm1.put("cf", "cq", "value");

            ConditionalMutation cm2 = new ConditionalMutation("put_several_dependent");
            cm2.addCondition(new Condition("cf", "cq").setValue("value"));
            cm2.putDelete("cf", "cq");

            CompletionStage<Result> op = writer.submit(cm1)
                    .thenSubmit(cm2)
                    .thenSubmit(cm1)
                    .thenSubmit(cm2);

            Result result = op.toCompletableFuture().get();
            assertEquals(Status.ACCEPTED, result.getStatus());
        }
    }

    @Test
    public void ensureExceedingCapacityFailsCleanly() throws Exception {

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

            CompletionStage<Result> op = writer.submit(cm1).thenSubmit(cm2);

            Result result = op.toCompletableFuture().get();
            assertEquals(Status.ACCEPTED, result.getStatus());
        }
    }

    @Test
    public void trySubmitMutationExceedingCapacity() throws Exception {

        AsyncConditionalWriterConfig config = AsyncConditionalWriterConfig.create()
                .withLimitedMemoryCapacity(512);

        try (AsyncConditionalWriter writer = asyncConnector.createConditionalWriter("table", config)) {

            // large enough payload so two mutations exceed capacity
            byte[] payload = new byte[768];

            ConditionalMutation cm = new ConditionalMutation("immediately_fail_exceeding_capacity");
            cm.addCondition(new Condition("cf", "cq"));
            cm.put("cf".getBytes(UTF_8), "cq".getBytes(UTF_8), payload);

            CompletionStage<Result> op = writer.trySubmit(cm);
            assertTrue(op.toCompletableFuture().isCompletedExceptionally());

        }
    }

    @Test
    public void testRetry() throws Exception {

        AsyncConditionalWriterConfig config = AsyncConditionalWriterConfig.create();

        try (AsyncConditionalWriter writer = asyncConnector.createConditionalWriter("table", config)) {

            ConditionalMutation someOtherMutation = new ConditionalMutation("red_herring");
            someOtherMutation.addCondition(new Condition("cf", "cq"));
            someOtherMutation.put("cf", "cq", "");

            ConditionalMutation cm = new ConditionalMutation("retry_row");
            cm.addCondition(new Condition("cf", "cq"));
            cm.put("cf", "cq", "");

            // write a mutation, but pretend we wrote the other one, got a result of UNKNOWN,
            // and that the mutation was not actually written
            WriteStage<Result> op = writer.submit(someOtherMutation)
                    .thenApply(result -> Results.withStatus(result, UNKNOWN))
                    .thenCompose(retryOnUnknown(cm, writer));

            assertEquals(Status.ACCEPTED, op.toCompletableFuture().join().getStatus());
        }
    }

    private Function<Result, CompletionStage<Result>> retryOnUnknown(ConditionalMutation cm, AsyncConditionalWriter writer) {
        return (result) -> {
            switch (getStatusUnchecked(result)) {
                // any non-exceptional completion continues down the nominal path
                // the only way to short-circuit over downstream composed stages is exceptional completion
                // otherwise some data needs to be passed along with the completed value that each stage
                // uses to make a decision of whether or not to do any work
                case ACCEPTED:
                    return writer.asSingleStage(completedFuture(result));
                case UNKNOWN:
                    AsyncScanner scanner = asyncConnector.createScanBuilder("table")
                            .range(Range.exact("retry_row"))
                            .isolation(true)
                            .build();
                    return scanner.toList().thenCompose(x -> failOnInterrupt(() -> writer.submit(cm)));
                default:
                    return writer.asSingleStage(immediatelyFailed(ResultException.of(result)));
            }
        };
    }

}
