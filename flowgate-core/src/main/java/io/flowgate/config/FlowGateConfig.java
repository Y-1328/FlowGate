package io.flowgate.config;

import java.util.List;
import java.util.Objects;
import javax.naming.ConfigurationException;


public final class FlowGateConfig {

    private final int globalConcurrency;
    private final List<QueueConfig> queues;

    public FlowGateConfig(int globalConcurrency, List<QueueConfig> queues) {
        if (globalConcurrency <= 0) {
            throw new IllegalArgumentException(
                    "Global concurrency must be positive"
            );
        }

        Objects.requireNonNull(queues, "queues must not be null");

        if (queues.isEmpty()) {
            throw new IllegalArgumentException(
                    "At least one queue must be configured"
            );
        }

        this.globalConcurrency = globalConcurrency;
        this.queues = List.copyOf(queues);
    }

    public int globalConcurrency() {
        return globalConcurrency;
    }

    public List<QueueConfig> queues() {
        return queues;
    }
}