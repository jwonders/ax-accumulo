package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.client.ConditionalWriter.Result;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.CompletionException;

/**
 * Indicates results were obtained, but the failure policy dictated that
 * one or more of the statuses was to be considered exceptional.
 */
public class ResultBatchException extends CompletionException implements Serializable {

    private final Collection<Result> results;

    public ResultBatchException(Collection<Result> results) {
        this.results = results;
    }

    public Collection<Result> getResults() {
        return results;
    }

    @Override
    public String toString() {
        return "ResultBatchException{" +
                "results=" + results +
                '}';
    }

}
