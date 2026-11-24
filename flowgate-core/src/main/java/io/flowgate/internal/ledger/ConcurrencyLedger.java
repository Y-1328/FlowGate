
package io.flowgate.internal.ledger;

import io.flowgate.internal.queue.QueueRegistry;
import io.flowgate.internal.queue.WorkloadQueue;

import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Centralized allocator for execution concurrency across configured queues.
 *
 * <p>Enforces the global concurrency budget, per-queue reservations,
 * and per-queue maximum concurrency. Unused reserved capacity may be
 * borrowed without consuming capacity needed by other queues with
 * observed unmet reservation demand.</p>
 *
 * <p>All allocation and release operations are serialized by one lock.
 * Waiting counts are supplied as snapshots by the caller.</p>
 *
 * <p>Queue indices and concurrency limits are derived from the same
 * QueueRegistry used by the runtime.</p>
 */
public final class ConcurrencyLedger {

    private final int globalConcurrency;
    private final int queueCount;

    private final int[] reserved;
    private final int[] max;
    private final int[] active;

    private final ReentrantLock lock = new ReentrantLock();

    private int totalActive;

    /**
     * Holds concurrency limits extracted from the queue registry.
     */
    private record LedgerLimits(int[] reserved, int[] maximum) {
    }

    /**
     * Creates the ledger from the runtime queue registry.
     *
     * <p>Queue ordering and indices are inherited directly from the
     * registry, ensuring consistent queue identity between scheduling
     * and resource allocation.</p>
     *
     * @param globalConcurrency global execution limit
     * @param registry runtime queue registry
     * @throws NullPointerException if registry is null
     * @throws IllegalArgumentException if configuration is invalid
     */
    public ConcurrencyLedger(
            int globalConcurrency,
            QueueRegistry registry
    ) {
        this(globalConcurrency, extractLimits(registry));
    }

    /**
     * Initializes the ledger using extracted registry limits.
     */
    private ConcurrencyLedger(
            int globalConcurrency,
            LedgerLimits limits
    ) {
        this(
                globalConcurrency,
                limits.reserved(),
                limits.maximum()
        );
    }

    /**
     * Extracts and validates queue concurrency limits.
     *
     * <p>The registry's defensive queue-array copy is obtained only once.
     * Queue ordering and index mapping are validated before extraction.</p>
     */
    private static LedgerLimits extractLimits(QueueRegistry registry) {

        Objects.requireNonNull(
                registry,
                "registry must not be null"
        );

        WorkloadQueue[] queues = registry.ordered();

        if (queues.length == 0) {
            throw new IllegalArgumentException(
                    "Queue registry must contain at least one queue"
            );
        }

        int[] reserved = new int[queues.length];
        int[] maximum = new int[queues.length];

        for (int i = 0; i < queues.length; i++) {

            WorkloadQueue queue = queues[i];

            if (queue == null || queue.index() != i) {
                throw new IllegalArgumentException(
                        "Queue registry index mismatch at position " + i
                );
            }

            reserved[i] = queue.config().reservedConcurrency();
            maximum[i] = queue.config().maxConcurrency();
        }

        return new LedgerLimits(reserved, maximum);
    }

    /**
     * Creates a ledger from explicit concurrency settings.
     *
     * @param globalConcurrency global execution limit
     * @param reserved reserved concurrency per queue
     * @param max maximum concurrency per queue
     * @throws NullPointerException if either array is null
     * @throws IllegalArgumentException if configuration is invalid
     */
    public ConcurrencyLedger(
            int globalConcurrency,
            int[] reserved,
            int[] max
    ) {

        Objects.requireNonNull(
                reserved,
                "reserved must not be null"
        );

        Objects.requireNonNull(
                max,
                "max must not be null"
        );

        if (globalConcurrency <= 0) {
            throw new IllegalArgumentException(
                    "Global concurrency must be positive"
            );
        }

        if (reserved.length == 0 || reserved.length != max.length) {
            throw new IllegalArgumentException(
                    "Reserved and maximum arrays must have equal, non-zero length"
            );
        }

        long reservedTotal = 0;

        for (int i = 0; i < reserved.length; i++) {

            if (reserved[i] < 0) {
                throw new IllegalArgumentException(
                        "Reserved concurrency must be non-negative at index " + i
                );
            }

            if (max[i] <= 0) {
                throw new IllegalArgumentException(
                        "Maximum concurrency must be positive at index " + i
                );
            }

            if (reserved[i] > max[i]) {
                throw new IllegalArgumentException(
                        "Reserved concurrency cannot exceed maximum at index " + i
                );
            }

            if (max[i] > globalConcurrency) {
                throw new IllegalArgumentException(
                        "Queue maximum cannot exceed global concurrency at index " + i
                );
            }

            reservedTotal += reserved[i];
        }

        if (reservedTotal > globalConcurrency) {
            throw new IllegalArgumentException(
                    "Total reserved concurrency cannot exceed global concurrency"
            );
        }

        this.globalConcurrency = globalConcurrency;
        this.queueCount = reserved.length;

        // Defensive copies prevent external mutation.
        this.reserved = reserved.clone();
        this.max = max.clone();

        this.active = new int[queueCount];
    }

    /**
     * Attempts to acquire an execution permit.
     *
     * @param queueIndex target queue index
     * @param waitingSnapshot observed waiting-task count per queue
     * @return granted permit, or null when denied
     */
    public ExecutionPermit tryAcquire(
            int queueIndex,
            int[] waitingSnapshot
    ) {

        int[] waiting = validatedSnapshot(
                queueIndex,
                waitingSnapshot
        );

        lock.lock();

        try {

            AllocationDecision decision =
                    decideLocked(queueIndex, waiting);

            if (!decision.isGranted()) {
                return null;
            }

            ExecutionPermit permit = new ExecutionPermit(
                    this,
                    queueIndex,
                    decision == AllocationDecision.GRANT_BORROWED
            );

            active[queueIndex]++;
            totalActive++;

            return permit;

        } finally {
            lock.unlock();
        }
    }

    /**
     * Evaluates whether a queue could acquire a permit.
     * Does not reserve capacity.
     *
     * @param queueIndex target queue index
     * @param waitingSnapshot observed waiting-task count per queue
     * @return allocation decision
     */
    public AllocationDecision check(
            int queueIndex,
            int[] waitingSnapshot
    ) {

        int[] waiting = validatedSnapshot(
                queueIndex,
                waitingSnapshot
        );

        lock.lock();

        try {
            return decideLocked(queueIndex, waiting);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Fills the eligibility array using one observed waiting snapshot.
     *
     * @param eligible destination eligibility array
     * @param waitingSnapshot observed waiting-task count per queue
     */
    public void snapshotEligibility(
            boolean[] eligible,
            int[] waitingSnapshot
    ) {

        Objects.requireNonNull(
                eligible,
                "eligible must not be null"
        );

        if (eligible.length != queueCount) {
            throw new IllegalArgumentException(
                    "Eligibility array length must equal queue count"
            );
        }

        int[] waiting = validatedSnapshot(waitingSnapshot);

        lock.lock();

        try {

            for (int i = 0; i < queueCount; i++) {
                eligible[i] = decideLocked(i, waiting).isGranted();
            }

        } finally {
            lock.unlock();
        }
    }

    /**
     * Evaluates allocation eligibility.
     *
     * Must be called while holding the ledger lock.
     */
    private AllocationDecision decideLocked(
            int queueIndex,
            int[] waiting
    ) {

        int free = globalConcurrency - totalActive;

        if (free < 1) {
            return AllocationDecision.DENY_GLOBAL_LIMIT;
        }

        if (active[queueIndex] >= max[queueIndex]) {
            return AllocationDecision.DENY_QUEUE_MAX;
        }

        // A queue can always consume its own reserved capacity.
        if (active[queueIndex] < reserved[queueIndex]) {
            return AllocationDecision.GRANT_RESERVED;
        }

        long held = 0;

        // Calculate capacity that must remain available for
        // other queues with observed unmet reservation demand.
        for (int i = 0; i < queueCount; i++) {

            if (i == queueIndex) {
                continue;
            }

            int unmetReservation = Math.max(
                    0,
                    reserved[i] - active[i]
            );

            held += Math.min(
                    unmetReservation,
                    waiting[i]
            );
        }

        // Borrow only if one slot can be consumed without
        // violating observed reservation demand.
        if (free > held) {
            return AllocationDecision.GRANT_BORROWED;
        }

        return AllocationDecision.DENY_RESERVED_FOR_OTHERS;
    }

    /**
     * Releases one active concurrency slot.
     * Called exclusively by ExecutionPermit.
     *
     * @param queueIndex queue whose permit is released
     */
    void release(int queueIndex) {

        lock.lock();

        try {

            validateQueueIndex(queueIndex);

            if (active[queueIndex] <= 0 || totalActive <= 0) {
                throw new IllegalStateException(
                        "Concurrency accounting underflow for queue " + queueIndex
                );
            }

            active[queueIndex]--;
            totalActive--;

        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the total number of active executions.
     */
    public int activeTotal() {

        lock.lock();

        try {
            return totalActive;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns currently available global execution capacity.
     */
    public int available() {

        lock.lock();

        try {
            return globalConcurrency - totalActive;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns active executions for a queue.
     *
     * @param queueIndex queue index
     */
    public int active(int queueIndex) {

        validateQueueIndex(queueIndex);

        lock.lock();

        try {
            return active[queueIndex];
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the number of currently borrowed execution slots
     * for a queue.
     *
     * @param queueIndex queue index
     */
    public int borrowed(int queueIndex) {

        validateQueueIndex(queueIndex);

        lock.lock();

        try {
            return Math.max(
                    0,
                    active[queueIndex] - reserved[queueIndex]
            );
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the configured queue count.
     */
    public int queueCount() {
        return queueCount;
    }

    /**
     * Returns the global concurrency budget.
     */
    public int globalConcurrency() {
        return globalConcurrency;
    }

    /**
     * Validates a waiting snapshot and the target queue index.
     */
    private int[] validatedSnapshot(
            int queueIndex,
            int[] waitingSnapshot
    ) {

        validateQueueIndex(queueIndex);

        return validatedSnapshot(waitingSnapshot);
    }

    /**
     * Validates and defensively copies a waiting snapshot.
     *
     * The caller must not mutate the input array while this
     * method is executing.
     */
    private int[] validatedSnapshot(int[] waitingSnapshot) {

        Objects.requireNonNull(
                waitingSnapshot,
                "waitingSnapshot must not be null"
        );

        if (waitingSnapshot.length != queueCount) {
            throw new IllegalArgumentException(
                    "Waiting snapshot length must equal queue count"
            );
        }

        int[] snapshot = waitingSnapshot.clone();

        for (int i = 0; i < snapshot.length; i++) {

            if (snapshot[i] < 0) {
                throw new IllegalArgumentException(
                        "Waiting count must be non-negative at index " + i
                );
            }
        }

        return snapshot;
    }

    /**
     * Validates a queue index.
     */
    private void validateQueueIndex(int queueIndex) {

        if (queueIndex < 0 || queueIndex >= queueCount) {
            throw new IllegalArgumentException(
                    "Invalid queue index: " + queueIndex
            );
        }
    }
}
