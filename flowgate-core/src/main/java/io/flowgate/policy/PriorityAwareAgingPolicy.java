
package io.flowgate.policy;

import io.flowgate.api.Priority;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Applies priority-specific aging ceilings to workload queues.
 *
 * <p>Each priority can have a different maximum aging boost.
 * Priorities without an explicit override use the configured default.</p>
 *
 * <p>This class is immutable and thread-safe.</p>
 */
public final class PriorityAwareAgingPolicy {

    private final long agingIntervalNanos;
    private final int defaultMaxBoost;
    private final Map<Priority, Integer> maxBoostByPriority;

    public PriorityAwareAgingPolicy(
            long agingIntervalNanos,
            int defaultMaxBoost,
            Map<Priority, Integer> maxBoostByPriority
    ) {
        if (agingIntervalNanos <= 0) {
            throw new IllegalArgumentException(
                    "Aging interval must be positive"
            );
        }

        if (defaultMaxBoost < 0) {
            throw new IllegalArgumentException(
                    "Default maximum boost cannot be negative"
            );
        }

        Objects.requireNonNull(
                maxBoostByPriority,
                "Priority boost configuration must not be null"
        );

        Map<Priority, Integer> copy = new HashMap<>();

        maxBoostByPriority.forEach((priority, maxBoost) -> {
            Objects.requireNonNull(priority, "Priority must not be null");
            Objects.requireNonNull(maxBoost, "Maximum boost must not be null");

            if (maxBoost < 0) {
                throw new IllegalArgumentException(
                        "Maximum boost cannot be negative"
                );
            }

            copy.put(priority, maxBoost);
        });

        this.agingIntervalNanos = agingIntervalNanos;
        this.defaultMaxBoost = defaultMaxBoost;
        this.maxBoostByPriority = Map.copyOf(copy);
    }

    /**
     * Calculates the effective scheduling quantum for a queue.
     *
     * @param priority queue priority
     * @param baseWeight configured queue weight
     * @param headWaitNanos age of the oldest waiting task
     * @return effective scheduling quantum
     */
    public int effectiveQuantum(
            Priority priority,
            int baseWeight,
            long headWaitNanos
    ) {
        Objects.requireNonNull(priority, "Priority must not be null");

        if (baseWeight <= 0) {
            throw new IllegalArgumentException(
                    "Base weight must be positive"
            );
        }

        if (headWaitNanos < 0) {
            throw new IllegalArgumentException(
                    "Head wait cannot be negative"
            );
        }

        int maxBoost = maxBoostByPriority.getOrDefault(
                priority,
                defaultMaxBoost
        );

        long earnedLevels = headWaitNanos / agingIntervalNanos;

        long boost = 1L + Math.min(
                earnedLevels,
                (long) maxBoost
        );

        long quantum = (long) baseWeight * boost;

        return (int) Math.min(quantum, Integer.MAX_VALUE);
    }

    public int maxBoostFor(Priority priority) {
        Objects.requireNonNull(priority, "Priority must not be null");

        return maxBoostByPriority.getOrDefault(
                priority,
                defaultMaxBoost
        );
    }

    public long agingIntervalNanos() {
        return agingIntervalNanos;
    }

    public int defaultMaxBoost() {
        return defaultMaxBoost;
    }
}
