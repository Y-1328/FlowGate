
package io.flowgate.internal.ledger;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Represents one granted execution-concurrency slot.
 *
 * <p>A permit must be closed when the associated execution finishes.
 * Closing a permit more than once is safe and releases its capacity
 * exactly once.</p>
 *
 * <p>Permits are issued exclusively by {@link ConcurrencyLedger}.</p>
 */
public final class ExecutionPermit implements AutoCloseable {

    private static final AtomicIntegerFieldUpdater<ExecutionPermit> RELEASED =
            AtomicIntegerFieldUpdater.newUpdater(
                    ExecutionPermit.class, "released"
            );

    private final ConcurrencyLedger ledger;
    private final int queueIndex;
    private final boolean borrowed;

    @SuppressWarnings("unused")
    private volatile int released;

    ExecutionPermit(
            ConcurrencyLedger ledger,
            int queueIndex,
            boolean borrowed
    ) {
        this.ledger = ledger;
        this.queueIndex = queueIndex;
        this.borrowed = borrowed;
    }

    /**
     * Returns the index of the queue owning this permit.
     *
     * @return stable queue index
     */
    public int queueIndex() {
        return queueIndex;
    }

    /**
     * Indicates whether this permit was granted using borrowed capacity.
     *
     * <p>This records the grant-time classification. It is diagnostic
     * metadata and does not represent the queue's current borrowed count.</p>
     *
     * @return true if this permit was initially borrowed
     */
    public boolean wasBorrowed() {
        return borrowed;
    }

    /**
     * Returns whether this permit has already been released.
     *
     * @return true if closed
     */
    public boolean isReleased() {
        return released != 0;
    }

    /**
     * Releases the associated concurrency slot exactly once.
     */
    @Override
    public void close() {
        if (RELEASED.compareAndSet(this, 0, 1)) {
            ledger.release(queueIndex);
        }
    }
}
