package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.security.Authorizations;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class AsyncConditionalWriterConfig {

    private final ConditionalWriterConfig config;

    @Nullable
    private Integer maxMutationsInFlight;

    @Nullable
    private Long maxBytesInFlight;

    private AsyncConditionalWriterConfig(ConditionalWriterConfig config) {
        this.config = config;
    }

    private AsyncConditionalWriterConfig(ConditionalWriterConfig config, Integer maxMutationsInFlight, Long maxBytesInFlight) {
        this.config = config;
        this.maxMutationsInFlight = maxMutationsInFlight;
        this.maxBytesInFlight = maxBytesInFlight;
    }

    public AsyncConditionalWriterConfig withAuthorizations(Authorizations auth) {
        ConditionalWriterConfig config = deepCopy(this.config).setAuthorizations(auth);
        return new AsyncConditionalWriterConfig(config, this.maxMutationsInFlight, this.maxBytesInFlight);
    }

    public AsyncConditionalWriterConfig withTimeout(long timeout, TimeUnit unit) {
        ConditionalWriterConfig config = deepCopy(this.config).setTimeout(timeout, unit);
        return new AsyncConditionalWriterConfig(config, this.maxMutationsInFlight, this.maxBytesInFlight);
    }

    public AsyncConditionalWriterConfig withMaxWriteThreads(int maxWriteThreads) {
        ConditionalWriterConfig config = deepCopy(this.config).setMaxWriteThreads(maxWriteThreads);
        return new AsyncConditionalWriterConfig(config, this.maxMutationsInFlight, this.maxBytesInFlight);
    }

    public AsyncConditionalWriterConfig withDurability(Durability durability) {
        ConditionalWriterConfig config = deepCopy(this.config).setDurability(durability);
        return new AsyncConditionalWriterConfig(config, this.maxMutationsInFlight, this.maxBytesInFlight);
    }

    public AsyncConditionalWriterConfig withClassLoaderContext(String classLoaderContext) {
        ConditionalWriterConfig config = deepCopy(this.config);
        config.setClassLoaderContext(classLoaderContext);
        return new AsyncConditionalWriterConfig(config, this.maxMutationsInFlight, this.maxBytesInFlight);
    }

    public AsyncConditionalWriterConfig withLimitedIncompleteMutations(int maxMutationsInFlight) {
        if (maxMutationsInFlight <= 0) {
            throw new IllegalArgumentException("Limit on incomplete mutations must be strictly positive.");
        }
        return new AsyncConditionalWriterConfig(config, maxMutationsInFlight, maxBytesInFlight);
    }

    public AsyncConditionalWriterConfig withLimitedMemoryCapacity(long maxBytesInFlight) {
        if (maxBytesInFlight <= 0) {
            throw new IllegalArgumentException("Limit on incomplete mutation size must be strictly positive.");
        }
        return new AsyncConditionalWriterConfig(config, maxMutationsInFlight, maxBytesInFlight);
    }

    public Optional<Integer> getIncompleteMutationsLimit() {
        return Optional.ofNullable(maxMutationsInFlight);
    }

    public Optional<Long> getMemoryCapacityLimit() {
        return Optional.ofNullable(maxBytesInFlight);
    }

    public ConditionalWriterConfig getConditionalWriterConfig() {
        return config;
    }

    public static AsyncConditionalWriterConfig create() {
        return createFrom(new ConditionalWriterConfig());
    }

    public static AsyncConditionalWriterConfig createFrom(ConditionalWriterConfig config) {
        return new AsyncConditionalWriterConfig(config);
    }

    private static ConditionalWriterConfig deepCopy(ConditionalWriterConfig config) {
        ConditionalWriterConfig copy = new ConditionalWriterConfig();
        copy.setAuthorizations(config.getAuthorizations());
        copy.setClassLoaderContext(config.getClassLoaderContext());
        copy.setDurability(config.getDurability());
        copy.setMaxWriteThreads(config.getMaxWriteThreads());
        copy.setTimeout(config.getTimeout(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);
        return copy;
    }

}
