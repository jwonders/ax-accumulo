package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.security.Authorizations;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public interface UpdateRequest {

    String getTable();
    Authorizations getAuthorizations();
    ConditionalMutation getMutation();
    Executor getSubmissionExecutor();
    Durability getDurability();

    long getSubmissionTimeout();
    TimeUnit getSubmissionTimeoutUnit();

}
