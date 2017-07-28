package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.client.ConditionalWriter.Result;

import java.util.Collection;
import java.util.concurrent.Future;

public interface ConditionalBatchWriteFuture extends ConditionalBatchWriteStage, Future<Collection<Result>> {

}
