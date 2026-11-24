package io.flowgate.api;

import io.flowgate.config.QueueConfig;
import io.flowgate.policy.RetryPolicy;

import java.time.Duration;
import java.util.Objects;

public final class QueueBuilder {

    private final String id;

    private Priority priority = Priority.NORMAL;
    private int weight = 1;
    private int capacity = 256;
    private int reservedConcurrency = 0;

    // -1 means inherit global concurrency
    private int maxConcurrency = -1;

    private Duration defaultDeadline;

    // Retries disabled by default
    private RetryPolicy retryPolicy = RetryPolicy.disabled();

    QueueBuilder(String id) {
        this.id = Objects.requireNonNull(id, "id");
    }

    public QueueBuilder priority(Priority priority) {
        this.priority = Objects.requireNonNull(priority, "priority");
        return this;
    }

    public QueueBuilder weight(int weight) {
        this.weight = weight;
        return this;
    }

    public QueueBuilder capacity(int capacity) {
        this.capacity = capacity;
        return this;
    }

    public QueueBuilder reservedConcurrency(int reservedConcurrency) {
        this.reservedConcurrency = reservedConcurrency;
        return this;
    }

    public QueueBuilder maxConcurrency(int maxConcurrency) {
        this.maxConcurrency = maxConcurrency;
        return this;
    }

    public QueueBuilder defaultDeadline(Duration defaultDeadline) {
        this.defaultDeadline = defaultDeadline;
        return this;
    }

    /**
     * Configures retry behaviour for tasks submitted to this queue.
     * Retries are disabled by default.
     */
    public QueueBuilder retryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = Objects.requireNonNull(
                retryPolicy,
                "retryPolicy"
        );
        return this;
    }

    QueueConfig build(int globalConcurrency) {

        int resolvedMaxConcurrency =
                maxConcurrency == -1
                        ? globalConcurrency
                        : maxConcurrency;

        return new QueueConfig(
                id,
                priority,
                weight,
                capacity,
                reservedConcurrency,
                resolvedMaxConcurrency,
                defaultDeadline,
                retryPolicy
        );
    }
}