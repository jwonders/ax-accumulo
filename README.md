# ax-accumulo

Asynchronous extensions to Accumulo

The ax-accumulo project provides an asynchronous programming model in
front of existing public Accumulo APIs.  It relies heavily on Java 8 
`CompletionStage` and `CompletableFuture` to communicate results to 
the original caller and allow for composition of dependent actions.  
It aims to align with the Java 9 `Flow` contracts when asynchronous 
streams are involved to simplify bridging with reactive stream 
libraries.

## Why

There are a number of problems that can benefit from highly concurrent 
multi-step operations across multiple Accumulo rows.  Throughput is 
essential, but minimizing end-to-end latency is an important secondary 
goal.

Accumulo is fundamentally capable of achieving very good performance for
this type of problem.  However, it can be cumbersome to implement a 
solution with the existing APIs and there are pitfalls to avoid.

## Disclaimer

This project is a work-in-progress and should not be considered stable.
Anyone who chooses to use it does so at their own risk.

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
with ways to construct async writers and async scanners.

## Async Writers

Creating an `AsyncConditionalWriter` is similar to creating a `ConditionalWriter`
using the Accumulo API.  First, create an `AsyncConditionalWriterConfig`.
It is similar to the `ConditionalWriterConfig` but also supports configuring
a limit for the memory consumed by in-flight submitted mutations.

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
    .thenSubmit(b)
    .thenSubmit(c);
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

### Failure Policy

A conditional write operation can return with several status codes.  Since
an exception is not raised for statuses such as `REJECTED`, or `UNKNOWN`,
the write stages will complete normally.  Often applications need to handle 
these cases differently and in some cases it is most natural to treat these
statuses as an exceptional completion so any downstream composed stages are
skipped.

A `FailurePolicy` allows the application to configure when a status is 
considered normal or exceptional.  By default, the failure policy of a
writer considers every status normal.

A common case is to only consider the `ACCEPTED` status a normal completion
and deal with special cases as part of exceptional completion handlers.  A
writer can be configured with such a failure policy in which case all writes
will complete normally only if their result status is `ACCEPTED`.

``` java
AsyncConditionalWriter axWriter = axConnector.createConditionalWriter("table", config)
    .withFailurePolicy(FailurePolicy.failUnlessAccepted());
```

The failure policy can be customized as needed.  Any writers derived from
an existing writer through `withRateLimit` will inherit the failure policy.
Similarly when configuring a failure policy, the returned writer will share
the rate limiter.

### Async Scanning

The asynchronous scanning API is designed around the Java 9 `Flow` API
which provides standard building blocks to connect asynchronous scans to 
libraries that provide reactive operators for manipulating asynchronous
streams.

| ax-accumulo class  | Behaves As              |
|--------------------|-------------------------|
| `AsyncScanner`     | `Flow.Publisher<Cell>`  |
| `ScanSubscriber`   | `Flow.Subscriber<Cell>` |
| `ScanSubscription` | `Flow.Subscription`     |

The `AsyncScanner` acts like an asynchronous `Iterable` in that it can
generate multiple scans from the same recipe.  Of course, if the data
is actively being modified, there is no isolation across scans just as
Accumulo does not provide isolation across rows.

Each call to `subscribe(ScanSubscriber)` will initiate a separate scan.


``` java
AsyncConnector connector = ...

AsyncScanner scanner = asyncConnector.createScanBuilder("table")
    .range(Range.exact("row"))
    .isolation(true)
    .build();

ScanSubscriber subscriber = ...
scanner.subscribe(subscriber);
```

In many cases, all cells within the scanned range need to be collected
prior to processing.  To simplify this case, the library provides some
convenience methods for asynchronously collecting cells.

``` java
AsyncConnector connector = ...

AsyncScanner scanner = asyncConnector.createScanBuilder("table")
    .range(Range.exact("row"))
    .isolation(true)
    .build();
    
CompletableFuture<List<Cell>> list = scanner.toList();
CompletableFuture<SortedSet<Cell>> set = scanner.toSortedSet();
CompletableFuture<SortedMap<Key, Value>> map = scanner.toSortedMap();
```