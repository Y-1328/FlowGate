
package io.flowgate.policy;

/**
 * Calculates bounded scheduling boosts based on queue waiting time.
 *
 * <p>The policy is independent of the scheduler. It calculates an
 * effective quantum that can be passed to the DWRR scheduler.</p>
 *
 * <p>Use a separate instance or configuration for each priority class
 * when different aging ceilings are required.</p>
 *
 * <p>This class is immutable and thread-safe.</p>
 */
public final class PriorityAgingPolicy {

    private final long agingIntervalNanos;
    private final int maxBoost;

    /**
     * Creates a bounded priority aging policy.
     *
     * @param agingIntervalNanos time required to earn one boost level
     * @param maxBoost maximum additional boost levels
     */
    public PriorityAgingPolicy(long agingIntervalNanos, int maxBoost) {
        if (agingIntervalNanos <= 0) {
            throw new IllegalArgumentException(
                    "Aging interval must be positive"
            );
        }

        if (maxBoost < 0) {
            throw new IllegalArgumentException(
                    "Maximum boost cannot be negative"
            );
        }

        this.agingIntervalNanos = agingIntervalNanos;
        this.maxBoost = maxBoost;
    }

    /**
     * Calculates the bounded aging boost for a queue.
     *
     * @param headWaitNanos time the oldest waiting task has waited
     * @return boost multiplier, always between 1 and maxBoost + 1
     */
    public int boost(long headWaitNanos) {
        if (headWaitNanos < 0) {
            throw new IllegalArgumentException(
                    "Head wait cannot be negative"
            );
        }

        long earnedLevels = headWaitNanos / agingIntervalNanos;

        return 1 + (int) Math.min(earnedLevels, maxBoost);
    }

    /**
     * Calculates the effective scheduling quantum.
     *
     * @param baseWeight configured queue weight
     * @param headWaitNanos time the oldest waiting task has waited
     * @return effective quantum, capped at Integer.MAX_VALUE
     */
    public int effectiveQuantum(int baseWeight, long headWaitNanos) {
        if (baseWeight <= 0) {
            throw new IllegalArgumentException(
                    "Base weight must be positive"
            );
        }

        long effective = (long) baseWeight * boost(headWaitNanos);

        return (int) Math.min(effective, Integer.MAX_VALUE);
    }

    public long agingIntervalNanos() {
        return agingIntervalNanos;
    }

    public int maxBoost() {
        return maxBoost;
    }
}
