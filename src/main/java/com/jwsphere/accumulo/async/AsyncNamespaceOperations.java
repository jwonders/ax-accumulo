package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.client.admin.NamespaceOperations;

import java.util.concurrent.ExecutorService;

public class AsyncNamespaceOperations {

    private final NamespaceOperations namespaceOps;
    private final ExecutorService executor;

    public AsyncNamespaceOperations(NamespaceOperations namespaceOps, ExecutorService executor) {
        this.namespaceOps = namespaceOps;
        this.executor = executor;
    }



}
