
package io.flowgate.config;

import io.flowgate.api.Priority;
import io.flowgate.policy.RetryPolicy;

import java.time.Duration;
import java.util.Objects;
import java.util.regex.Pattern;

public final class QueueConfig {

    private final String id;
    private final Priority priority;
    private final int weight;
    private final int capacity;
    private final int reservedConcurrency;
    private final int maxConcurrency;
    private final Duration defaultDeadline;
    private final RetryPolicy retryPolicy;
    public static final Pattern VALID_ID =
            Pattern.compile("[A-Za-z0-9._-]{1,64}");

    /**
     * Creates a queue configuration without retries.
     */
    public QueueConfig(
            String id,
            Priority priority,
            int weight,
            int capacity,
            int reservedConcurrency,
            int maxConcurrency,
            Duration defaultDeadline
    ) {
        this(
                id,
                priority,
                weight,
                capacity,
                reservedConcurrency,
                maxConcurrency,
                defaultDeadline,
                RetryPolicy.disabled()
        );
    }

    /**
     * Creates a queue configuration with an explicit retry policy.
     *
     * @param retryPolicy retry behaviour for tasks of this queue;
     *                    null is treated as {@link RetryPolicy#disabled()}
     */
    public QueueConfig(
            String id,
            Priority priority,
            int weight,
            int capacity,
            int reservedConcurrency,
            int maxConcurrency,
            Duration defaultDeadline,
            RetryPolicy retryPolicy
    ) {
        this.id = Objects.requireNonNull(id, "id must not be null");
        this.priority = Objects.requireNonNull(
                priority, "priority must not be null"
        );

        if (id.isBlank()) {
            throw new IllegalArgumentException("Queue ID must not be blank");
        }
        if (weight <= 0) {
            throw new IllegalArgumentException("Weight must be positive");
        }
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive");
        }
        if (reservedConcurrency < 0) {
            throw new IllegalArgumentException(
                    "Reserved concurrency must be non-negative"
            );
        }
        if (maxConcurrency <= 0) {
            throw new IllegalArgumentException(
                    "Maximum concurrency must be positive"
            );
        }
        if (reservedConcurrency > maxConcurrency) {
            throw new IllegalArgumentException(
                    "Reserved concurrency cannot exceed maximum concurrency"
            );
        }

        if (defaultDeadline != null &&
                (defaultDeadline.isZero() || defaultDeadline.isNegative())) {
            throw new IllegalArgumentException(
                    "Default deadline must be positive"
            );
        }

        this.weight = weight;
        this.capacity = capacity;
        this.reservedConcurrency = reservedConcurrency;
        this.maxConcurrency = maxConcurrency;
        this.defaultDeadline = defaultDeadline;
        this.retryPolicy = retryPolicy != null
                ? retryPolicy
                : RetryPolicy.disabled();
    }

    public String id() {
        return id;
    }

    public Priority priority() {
        return priority;
    }

    public int weight() {
        return weight;
    }

    public int capacity() {
        return capacity;
    }

    public int reservedConcurrency() {
        return reservedConcurrency;
    }

    public int maxConcurrency() {
        return maxConcurrency;
    }

    public Duration defaultDeadline() {
        return defaultDeadline;
    }

    /** Retry behaviour for this queue; {@link RetryPolicy#disabled()} unless configured. */
    public RetryPolicy retryPolicy() {
        return retryPolicy;
    }
}
