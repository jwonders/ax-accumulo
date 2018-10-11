package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.client.ConditionalWriter.Status;

import java.util.concurrent.CompletionException;

public class Results {

    private Results() {
        // hiding implicit default constructor
    }

    public static Result withStatus(Result result, Status status) {
        return new Result(status, result.getMutation(), result.getTabletServer());
    }

    public static Status getStatusUnchecked(Result result) {
        try {
            return result.getStatus();
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new CompletionException(e);
        }
    }

}
