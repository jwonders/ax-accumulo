package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.client.ConditionalWriter.Result;

import java.io.Serializable;
import java.util.concurrent.CompletionException;

/**
 * Indicates a result was obtained, but the failure policy dictated that
 * the status was to be considered exceptional.
 */
public class ResultException extends CompletionException implements Serializable {

    private final Result result;

    public ResultException(Result result) {
        this.result = result;
    }

    public Result getResult() {
        return result;
    }

    @Override
    public String toString() {
        return "ResultException{" +
                "result=" + result +
                '}';
    }

}
