package com.jwsphere.accumulo.async;

public class CapacityExceededException extends IllegalArgumentException {

    private final long capacityRequired;
    private final long capacityLimit;

    public CapacityExceededException(long capacityRequired, long capacityLimit) {
        this.capacityRequired = capacityRequired;
        this.capacityLimit = capacityLimit;
    }

    public long getCapacityRequired() {
        return capacityRequired;
    }

    public long getCapacityLimit() {
        return capacityLimit;
    }

    @Override
    public String toString() {
        return "Required capacity of " + capacityRequired + " exceeds limit of " + capacityLimit;
    }

}
