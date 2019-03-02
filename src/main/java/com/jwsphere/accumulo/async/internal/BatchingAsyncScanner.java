package com.jwsphere.accumulo.async.internal;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;

public class BatchingAsyncScanner {

    private final Connector connector;
    private final BatchScanner scanner;

    public BatchingAsyncScanner(Connector connector, String table, Authorizations auth, int threads) {
        this.connector = connector;
        try {
            this.scanner = connector.createBatchScanner(table, auth, threads);


        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }



}
