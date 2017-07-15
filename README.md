# ax-accumulo

Asynchronous extensions to Accumulo

The ax-accumulo project wraps existing Accumulo APIs for asynchronous execution 
in background threads.  It relies heavily on Java 8 `CompletionStage` and
`CompletableFuture` to communicate results to the original caller and allow
for composition of dependent actions.

## Why

There are classes of problems that involve highly concurrent multi-step
operations across multiple Accumulo rows.  Throughput is essential, but
minimizing end-to-end latency is an important secondary goal.

Accumulo is fundamentally capable of achieving very good performance for
this type of problem.  However, it can be cumbersome to implement a 
solution with the existing APIs and there are pitfalls to avoid.

## Disclaimer

This project is a work-in-progress and should not be considered stable.

## Entry Point

The entry point to functionality is the `AsyncConnector` which can wrap an
existing Accumulo `Connector`, optionally with a default `Executor` for async
operations.  By default the executor will be the `ForkJoinPool.commonPool()`

    Connector connector = ...
    AsyncConnector.wrap(connector);

    Executor executor = task -> task.run()
    AsyncConnector.wrap(connector, executor)

The `AsyncConnector` exposes an API that parallels the Accumulo APIs but
performs operations asynchronously.  You will see methods for async table
operations, async security operations, and async namespace operations along
with ways to construct async writers.


## Async Writers

Creating an `AsyncConditionalWriter` is similar to creating a `ConditionalWriter`
using the Accumulo API.  First, create an `AsyncConditionalWriterConfig`.
It is similar to the `ConditionalWriterConfig` but also supports configuring
a limit for the memory consumed by in-flight submitted mutations.  We don't
want to run out of memory now, do we.

It is certainly possible to limit memory usage at the application level.  
If you have another mechanism for avoiding out of memory errors, this can 
be set to `Long.MAX_VALUE`.

``` java
AsyncConditionalWriterConfig config = AsyncConditionalWriterConfig.create()
  .withAuthorizations(new Authorizations("A", "B", "C"))
  .withDurability(Durability.FLUSH)
  .withTimeout(1, TimeUnit.MINUTES)
  .withMaxWriteThreads(4)
  .withLimitedMemoryCapacity(10 * 1024 * 1024);
                
AsyncConditionalWriter axWriter = axConnector.createConditionalWriter("table", config);
```

An `AsyncConditionalWriter` simplifies writing mutations that are dependent
on each other as well as performing other actions upon notification that a 
write has completed.

``` java
ConditionalMutation a = new ConditionalMutation("row");
a.addCondition(new Condition("cf", "cq"));
a.put("cf", "cq", "a");

ConditionalMutation b = new ConditionalMutation("row");
b.addCondition(new Condition("cf", "cq").setValue("a"));
b.put("cf", "cq", "b");

ConditionalMutation c = new ConditionalMutation("row");
c.addCondition(new Condition("cf", "cq").setValue("b"));
c.put("cf", "cq", "c");

CompletionStage<Result> resultStage = axWriter.submit(a)
    .thenCompose(x -> failOnInterrupt(() -> axWriter.submit(b)))
    .thenCompose(x -> failOnInterrupt(() -> axWriter.submit(c)));
```

Occasionally it is necessary to perform some work asynchronously and then
wait for all of the previously submitted work to complete before moving on.
The amount of work might be very large and retaining references to futures 
could prevent memory from being reclaimed by GC in a timely manner.  The
`AsyncConditionalWriter` provides `await` methods that will block until all
previously submitted mutations have been written.  The completion is tracked
in a very memory-efficient way to avoid aforementioned problems.

``` java
axWriter.await();
```