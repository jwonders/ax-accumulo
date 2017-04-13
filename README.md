# ax-accumulo
Asynchronous extensions to Accumulo

## Usage

The usage is a work in progress, but the general feel should stay the same.

Wrap a `Connector` in an `AsyncConnector`.

``` java
Connector connector = ...
AsyncConnector axConnector = new AsyncConnector(connector); 
```
    
Create an `AsyncConditionalWriter`
``` java
ConditionalWriterConfig config = new ConditionalWriterConfig()
    .setAuthorizations(new Authorizations("A", "B", "C"))
    .setDurability(Durability.FLUSH)
    .setTimeout(1, TimeUnit.MINUTES)
    .setMaxWriteThreads(4);
                
AsyncConditionalWriter axWriter = axConnector.createConditionalWriter("table", config);
```

Write mutations that are dependent on each other and chain them together.

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

CompletionStage<Result> resultStage = axWriter.submit(a).toCompletableFuture()
    .thenCompose(x -> failOnInterrupt(() -> axWriter.submit(b)))
    .thenCompose(x -> failOnInterrupt(() -> axWriter.submit(c)));
```

Wait for previously submitted mutations to complete.

``` java
axWriter.await();
```