package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.client.ConditionalWriter.Result;

import java.io.Serializable;
import java.util.concurrent.CompletionException;

public class ResultException extends CompletionException implements Serializable {

    private final Result result;

    public ResultException(Result result) {
        this.result = result;
    }

    public Result getStatus() {
        return result;
    }

    @Override
    public String toString() {
        return "ResultException{" +
                "result=" + result +
                '}';
    }

}
