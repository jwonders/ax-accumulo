package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.security.Authorizations;

import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static com.jwsphere.accumulo.async.Errors.failOnInterrupt;

public class MiniAccumuloProvider {


    public static void main(String[] args) throws Exception {

        Connector connector = null;
        AsyncConnector axConnector = new AsyncConnector(connector);

        ConditionalWriterConfig config = new ConditionalWriterConfig()
                .setAuthorizations(new Authorizations("A", "B", "C"))
                .setDurability(Durability.FLUSH)
                .setTimeout(1, TimeUnit.MINUTES)
                .setMaxWriteThreads(4);

        AsyncConditionalWriter axWriter = axConnector.createConditionalWriter("table", config);

        ConditionalMutation a = new ConditionalMutation("row");
        a.addCondition(new Condition("cf", "cq"));
        a.put("cf", "cq", "a");

        ConditionalMutation b = new ConditionalMutation("row");
        b.addCondition(new Condition("cf", "cq").setValue("a"));
        b.put("cf", "cq", "b");

        ConditionalMutation c = new ConditionalMutation("row");
        c.addCondition(new Condition("cf", "cq").setValue("c"));
        c.put("cf", "cq", "c");

        axWriter.submit(a).toCompletableFuture()
                .thenCompose(x -> failOnInterrupt(() -> axWriter.submit(b)))
                .thenCompose(x -> failOnInterrupt(() -> axWriter.submit(c)));

    }

}
