package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.client.ConditionalWriter.Status;

import java.util.function.Predicate;

public final class FailurePolicy implements Predicate<Status> {

    private final boolean failOnRejected;
    private final boolean failOnUnknown;
    private final boolean failOnInvisibleVisibility;
    private final boolean failOnViolated;

    private FailurePolicy() {
        this(false, false, false, false);
    }

    private FailurePolicy(boolean failOnRejected, boolean failOnUnknown, boolean failOnInvisibleVisibility, boolean failOnViolated) {
        this.failOnRejected = failOnRejected;
        this.failOnUnknown = failOnUnknown;
        this.failOnInvisibleVisibility = failOnInvisibleVisibility;
        this.failOnViolated = failOnViolated;
    }

    @Override
    public boolean test(Status status) {
        switch (status) {
            case ACCEPTED:
                return true;
            case REJECTED:
                return !failOnRejected;
            case VIOLATED:
                return !failOnViolated;
            case UNKNOWN:
                return !failOnUnknown;
            case INVISIBLE_VISIBILITY:
                return !failOnInvisibleVisibility;
            default:
                return false;
        }
    }

    public boolean getFailOnRejected() {
        return failOnRejected;
    }

    public boolean getFailOnUnknown() {
        return failOnUnknown;
    }

    public boolean getFailOnInvisibleVisibility() {
        return failOnInvisibleVisibility;
    }

    public boolean getFailOnViolated() {
        return failOnViolated;
    }

    public FailurePolicy withFailOnRejected() {
        return withFailOnRejected(true);
    }

    public FailurePolicy withFailOnUnknown() {
        return withFailOnUnknown(true);
    }

    public FailurePolicy withFailOnInvisibleVisibility() {
        return withFailOnInvisibleVisibility(true);
    }

    public FailurePolicy withFailOnViolated() {
        return withFailOnViolated(true);
    }

    public FailurePolicy withFailOnRejected(boolean failOnRejected) {
        return new FailurePolicy(failOnRejected, failOnUnknown, failOnInvisibleVisibility, failOnViolated);
    }

    public FailurePolicy withFailOnUnknown(boolean failOnUnknown) {
        return new FailurePolicy(failOnRejected, failOnUnknown, failOnInvisibleVisibility, failOnViolated);
    }

    public FailurePolicy withFailOnInvisibleVisibility(boolean failOnInvisibleVisibility) {
        return new FailurePolicy(failOnRejected, failOnUnknown, failOnInvisibleVisibility, failOnViolated);
    }

    public FailurePolicy withFailOnViolated(boolean failOnViolated) {
        return new FailurePolicy(failOnRejected, failOnUnknown, failOnInvisibleVisibility, failOnViolated);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FailurePolicy that = (FailurePolicy) o;
        return failOnRejected == that.failOnRejected &&
                failOnUnknown == that.failOnUnknown &&
                failOnInvisibleVisibility == that.failOnInvisibleVisibility &&
                failOnViolated == that.failOnViolated;
    }

    @Override
    public int hashCode() {
        int result = (failOnRejected ? 1 : 0);
        result = 31 * result + (failOnUnknown ? 1 : 0);
        result = 31 * result + (failOnInvisibleVisibility ? 1 : 0);
        result = 31 * result + (failOnViolated ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "FailurePolicy{" +
                "failOnRejected=" + failOnRejected +
                ", failOnUnknown=" + failOnUnknown +
                ", failOnInvisibleVisibility=" + failOnInvisibleVisibility +
                ", failOnViolated=" + failOnViolated +
                '}';
    }

    /**
     * Creates a failure policy that treats every status as normal.
     */
    public static FailurePolicy allNormal() {
        return new FailurePolicy();
    }

    /**
     * Creates a failure policy that treats any status other than {@code Status.ACCEPTED}
     * as an exceptional case.
     */
    public static FailurePolicy failUnlessAccepted() {
        return new FailurePolicy(true, true, true, true);
    }

}
