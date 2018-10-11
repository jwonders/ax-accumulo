package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.client.ConditionalWriter.Result;

import java.io.Serializable;
import java.util.concurrent.CompletionException;

/**
 * Indicates a result was obtained, but the status was considered to
 * be a failure according to the failure policy.
 */
public class ResultException extends CompletionException implements Serializable {

    private final Result result;

    private ResultException(Result result) {
        this.result = result;
    }

    public Result getResult() {
        return result;
    }

    @Override
    public String toString() {
        return "ResultException{result=" + result + '}';
    }

    public static ResultException of(Result result) {
        return new ResultException(result);
    }

}
