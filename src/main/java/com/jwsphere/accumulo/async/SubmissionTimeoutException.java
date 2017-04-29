package com.jwsphere.accumulo.async;

import java.util.concurrent.CancellationException;

public class SubmissionTimeoutException extends CancellationException {

    public SubmissionTimeoutException() {
        super("Submission could not be completed within the allotted time.");
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        // since submission timeouts are used as an exceptional completion
        // the stack trace does not provide additional meaning and a single
        // exception instance can be reused
        return this;
    }

}
