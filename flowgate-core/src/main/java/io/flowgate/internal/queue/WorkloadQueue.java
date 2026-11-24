
package io.flowgate.internal.queue;

import io.flowgate.config.QueueConfig;
import io.flowgate.internal.task.TaskRecord;

import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;

/**
 * Bounded, lock-free, multi-producer FIFO storage for one workload.
 *
 * <p>Capacity is enforced using a CAS-bounded counter rather than the
 * underlying ConcurrentLinkedQueue, whose size() operation is O(n).
 *
 * <p>Admission uses a two-phase reservation process:
 * <ol>
 *     <li>{@link #tryReserve()} claims one local queue slot and one global slot.</li>
 *     <li>The caller must either publish a task using
 *         {@link #enqueueReserved(TaskRecord)} or release the reservation
 *         using {@link #abortReservation()}.</li>
 * </ol>
 *
 * <p>Exactly one of enqueueReserved() or abortReservation() must follow
 * every successful reservation. Neither operation may be called twice
 * for the same reservation.
 *
 * <p>{@link #poll()} releases both local and global admission capacity
 * when a task leaves the waiting queue, not when its execution completes.
 *
 * <p>Invariants:
 * <ul>
 *     <li>size() never exceeds the configured queue capacity.</li>
 *     <li>size() counts reserved slots as well as published tasks.</li>
 *     <li>size() may temporarily exceed the number of visible elements.</li>
 *     <li>Consequently, poll() may return null while size() is positive.</li>
 * </ul>
 *
 * <p>Consumers must treat a null result from poll() as temporarily having
 * no visible task, not as proof that the queue is empty.
 */
public final class WorkloadQueue {

    public enum Reservation {
        RESERVED,
        QUEUE_FULL,
        GLOBAL_LIMIT,
        CLOSED
    }

    private final int index;
    private final QueueConfig config;
    private final GlobalAdmissionGate gate;

    private final ConcurrentLinkedQueue<TaskRecord<?>> tasks =
            new ConcurrentLinkedQueue<>();

    private final AtomicInteger size = new AtomicInteger();

    public WorkloadQueue(
            int index,
            QueueConfig config,
            GlobalAdmissionGate gate
    ) {
        if (index < 0) {
            throw new IllegalArgumentException(
                    "index must be >= 0"
            );
        }

        this.index = index;
        this.config = Objects.requireNonNull(
                config,
                "config"
        );
        this.gate = Objects.requireNonNull(
                gate,
                "gate"
        );
    }

    /**
     * Attempts to reserve one local queue slot and one global admission slot.
     *
     * <p>The local slot is acquired first. If global admission fails,
     * the local reservation is rolled back.
     *
     * @return RESERVED if both slots were acquired; otherwise the reason
     *         admission could not proceed
     */
    public Reservation tryReserve() {

        // Reserve local queue capacity first.
        for (;;) {
            int currentSize = size.get();

            if (currentSize >= config.capacity()) {
                return Reservation.QUEUE_FULL;
            }

            if (size.compareAndSet(
                    currentSize,
                    currentSize + 1
            )) {
                break;
            }
        }

        // Then reserve global admission capacity.
        switch (gate.tryAcquire()) {

            case ACQUIRED:
                return Reservation.RESERVED;

            case LIMIT_REACHED:
                size.decrementAndGet();
                return Reservation.GLOBAL_LIMIT;

            default:
                size.decrementAndGet();
                return Reservation.CLOSED;
        }
    }

    /**
     * Publishes a task using a previously acquired reservation.
     *
     * <p>The caller transfers ownership of the reservation to this queue.
     * This operation must be called exactly once for each successful
     * reservation unless the reservation is instead aborted.
     *
     * @param record task record to publish
     * @throws NullPointerException if record is null
     */
    public void enqueueReserved(TaskRecord<?> record) {

        Objects.requireNonNull(record, "record");

        tasks.offer(record);
    }

    /**
     * Releases a previously acquired reservation that will not be used.
     *
     * <p>Must be called exactly once for each successful reservation
     * that was not published using {@link #enqueueReserved(TaskRecord)}.
     *
     * <p>Calling this method without an outstanding reservation is an
     * accounting error.
     */
    public void abortReservation() {

        size.decrementAndGet();
        gate.release();
    }

    /**
     * Removes the oldest visible task and releases its admission capacity.
     *
     * @return the oldest task, or null if no task is currently visible
     */
    public TaskRecord<?> poll() {

        TaskRecord<?> record = tasks.poll();

        if (record != null) {
            size.decrementAndGet();
            gate.release();
        }

        return record;
    }

    /**
     * Returns the oldest visible task without removing it.
     *
     * @return the oldest visible task, or null if none is visible
     */


    /**
     * Removes the exact task record if it is still waiting in this queue.
     *
     * <p>This operation is safe to race with poll(). Exactly one operation
     * can remove a published record. The operation that wins owns releasing
     * the queue and global admission reservations.</p>
     *
     * @param record exact task record to remove
     * @return true if removed; false if already removed or not present
     */
    public boolean remove(TaskRecord<?> record) {

        Objects.requireNonNull(record, "record");

        // Remove the exact published record.
        if (!tasks.remove(record)) {
            return false;
        }

        // Release capacity only if this operation actually removed it.
        size.decrementAndGet();
        gate.release();

        return true;
    }

    /**
     * Returns a snapshot of the currently published task records.
     *
     * <p>The snapshot is weakly consistent: concurrent enqueue and poll
     * operations may or may not be reflected in the returned list.</p>
     *
     * <p>Creating a snapshot does not remove tasks or release admission
     * capacity. Callers must use remove(record) to remove a task safely.</p>
     *
     * @return an immutable snapshot of visible task records
     */
    public List<TaskRecord<?>> snapshot() {
        return List.copyOf(tasks);
    }


    public TaskRecord<?> peek() {
        return tasks.peek();
    }

    /**
     * Returns the number of currently held local admission slots.
     *
     * <p>This includes reservations that have not yet been published.
     */
    public int size() {
        return size.get();
    }

    public int capacity() {
        return config.capacity();
    }

    public int index() {
        return index;
    }

    public String id() {
        return config.id();
    }

    public QueueConfig config() {
        return config;
    }
}
