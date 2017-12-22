package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.client.ConditionalWriter.Status;

import java.util.function.Predicate;

@FunctionalInterface
public interface StatusPredicate extends Predicate<Status> {

    static StatusPredicate isUnknown() {
        return status -> Status.UNKNOWN == status;
    }

    static StatusPredicate isRejected() {
        return status -> Status.REJECTED == status;
    }

    static StatusPredicate isAccepted() {
        return status -> Status.ACCEPTED == status;
    }

    static StatusPredicate isInvisibleVisibility() {
        return status -> Status.INVISIBLE_VISIBILITY == status;
    }

    static StatusPredicate isViolated() {
        return status -> Status.VIOLATED == status;
    }

}
