package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.client.admin.SecurityOperations;

import java.util.concurrent.ExecutorService;

public class AsyncSecurityOperations {

    private final SecurityOperations securityOps;
    private final ExecutorService executor;

    public AsyncSecurityOperations(SecurityOperations securityOps, ExecutorService executor) {
        this.securityOps = securityOps;
        this.executor = executor;
    }

}
