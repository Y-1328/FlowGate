
package io.flowgate.internal.ledger;

/**
 * Describes the outcome of a concurrency allocation decision.
 *
 * <p>A queue may receive a permit using its reserved concurrency
 * or by borrowing currently available global capacity.</p>
 */
public enum AllocationDecision {

    /** The queue received a permit within its reserved capacity. */
    GRANT_RESERVED,

    /** The queue received a permit by borrowing shared capacity. */
    GRANT_BORROWED,

    /** No global execution capacity is currently available. */
    DENY_GLOBAL_LIMIT,

    /** The queue has reached its configured maximum concurrency. */
    DENY_QUEUE_MAX,

    /** Capacity is protected for other queues with unmet reservations. */
    DENY_RESERVED_FOR_OTHERS;

    /**
     * Returns whether this decision allows execution capacity to be granted.
     *
     * @return true for grant decisions; false for denial decisions
     */
    public boolean isGranted() {
        return this == GRANT_RESERVED || this == GRANT_BORROWED;
    }
}
