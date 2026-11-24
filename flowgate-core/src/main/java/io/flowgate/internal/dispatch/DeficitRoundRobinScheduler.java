
package io.flowgate.internal.dispatch;

import java.util.Objects;

/**
 * Stateful Deficit Round Robin scheduler.
 *
 * <p>Each queue receives a scheduling quantum. Its deficit determines
 * how many successful dispatches it can consume before the scheduler
 * advances to the next queue.</p>
 *
 * <p>Selection and dispatch accounting are separate operations:
 * selectNext() selects a queue, while commitDispatch() consumes
 * one scheduling opportunity only after successful dispatch.</p>
 *
 * <p>If a selection cannot be dispatched, cancelSelection() abandons
 * it without consuming deficit.</p>
 *
 * <p>This class is intentionally independent of queue storage,
 * concurrency allocation, and task execution.</p>
 *
 * <p>Not thread-safe. Intended to be owned by a single dispatcher thread.</p>
 */
public final class DeficitRoundRobinScheduler {

    private final int queueCount;
    private final long[] deficits;

    /**
     * Index from which the next scheduling search begins.
     */
    private int cursor;

    /**
     * Queue currently selected but not yet committed.
     */
    private int pendingSelection = -1;

    /**
     * Deficit that will remain after committing the pending selection.
     */
    private long pendingDeficit;

    public DeficitRoundRobinScheduler(int queueCount) {
        if (queueCount <= 0) {
            throw new IllegalArgumentException(
                    "Queue count must be positive"
            );
        }

        this.queueCount = queueCount;
        this.deficits = new long[queueCount];
    }

    /**
     * Selects the next eligible queue without consuming its deficit.
     *
     * <p>The caller must subsequently invoke either commitDispatch()
     * or cancelSelection() before requesting another selection.</p>
     *
     * <p>A queue with no waiting tasks has its deficit reset.
     * A temporarily ineligible queue retains its deficit.</p>
     *
     * @param eligible whether each queue can currently receive a permit
     * @param waiting number of waiting tasks in each queue
     * @param quantum effective scheduling quantum for each queue
     * @return selected queue index, or -1 if no queue is eligible
     */
    public int selectNext(
            boolean[] eligible,
            int[] waiting,
            int[] quantum
    ) {
        Objects.requireNonNull(eligible, "eligible must not be null");
        Objects.requireNonNull(waiting, "waiting must not be null");
        Objects.requireNonNull(quantum, "quantum must not be null");

        validateInputs(eligible, waiting, quantum);

        if (pendingSelection != -1) {
            throw new IllegalStateException(
                    "Previous selection must be committed or cancelled"
            );
        }

        for (int i = 0; i < queueCount; i++) {
            if (waiting[i] == 0) {
                deficits[i] = 0;
            }
        }

        for (int offset = 0; offset < queueCount; offset++) {
            int index = (cursor + offset) % queueCount;

            if (!eligible[index] || waiting[index] == 0) {
                continue;
            }

            long availableDeficit = deficits[index] > 0
                    ? deficits[index]
                    : quantum[index];

            pendingSelection = index;
            pendingDeficit = availableDeficit - 1;

            return index;
        }

        return -1;
    }

    /**
     * Commits a successful dispatch for the selected queue.
     *
     * <p>Consumes exactly one scheduling opportunity.</p>
     *
     * @param queueIndex selected queue index
     */
    public void commitDispatch(int queueIndex) {
        validatePendingSelection(queueIndex);

        deficits[queueIndex] = pendingDeficit;

        if (pendingDeficit == 0) {
            cursor = (queueIndex + 1) % queueCount;
        } else {
            cursor = queueIndex;
        }

        clearPendingSelection();
    }

    /**
     * Abandons a selection that did not result in a dispatch.
     *
     * <p>Does not consume deficit or advance the cursor.</p>
     *
     * @param queueIndex selected queue index
     */
    public void cancelSelection(int queueIndex) {
        validatePendingSelection(queueIndex);
        clearPendingSelection();
    }

    /**
     * Resets all accumulated scheduling deficits and cursor state.
     */
    public void reset() {
        for (int i = 0; i < queueCount; i++) {
            deficits[i] = 0;
        }

        cursor = 0;
        clearPendingSelection();
    }

    /**
     * Returns the current committed deficit of a queue.
     */
    public long deficit(int queueIndex) {
        validateQueueIndex(queueIndex);
        return deficits[queueIndex];
    }

    private void validateInputs(
            boolean[] eligible,
            int[] waiting,
            int[] quantum
    ) {
        if (eligible.length != queueCount
                || waiting.length != queueCount
                || quantum.length != queueCount) {
            throw new IllegalArgumentException(
                    "All input arrays must match the configured queue count"
            );
        }

        for (int i = 0; i < queueCount; i++) {
            if (waiting[i] < 0) {
                throw new IllegalArgumentException(
                        "Waiting count cannot be negative at index " + i
                );
            }

            if (quantum[i] <= 0) {
                throw new IllegalArgumentException(
                        "Scheduling quantum must be positive at index " + i
                );
            }
        }
    }

    private void validatePendingSelection(int queueIndex) {
        if (pendingSelection == -1) {
            throw new IllegalStateException(
                    "There is no pending selection"
            );
        }

        if (pendingSelection != queueIndex) {
            throw new IllegalArgumentException(
                    "Queue index does not match pending selection"
            );
        }
    }

    private void clearPendingSelection() {
        pendingSelection = -1;
        pendingDeficit = 0;
    }

    private void validateQueueIndex(int queueIndex) {
        if (queueIndex < 0 || queueIndex >= queueCount) {
            throw new IllegalArgumentException(
                    "Invalid queue index: " + queueIndex
            );
        }
    }
}
