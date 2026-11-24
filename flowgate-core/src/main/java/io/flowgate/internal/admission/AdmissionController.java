
package io.flowgate.internal.admission;

import io.flowgate.api.RejectionReason;
import io.flowgate.api.Submission;
import io.flowgate.api.TaskStatus;
import io.flowgate.api.WorkloadTask;

import io.flowgate.internal.queue.GlobalAdmissionGate;
import io.flowgate.internal.queue.QueueRegistry;
import io.flowgate.internal.queue.WorkloadQueue;
import io.flowgate.internal.task.TaskRecord;
import io.flowgate.internal.time.TimeSource;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Controls whether a submitted workload is admitted into FlowGate.
 *
 * <p>Admission is bounded by both per-queue capacity and a global
 * admission limit. Tasks are created only after a reservation succeeds.
 *
 * <p>Each accepted task transitions to QUEUED before it becomes visible
 * to the dispatcher.
 */
public final class AdmissionController {

    private static final long MAX_DEADLINE_NANOS =
            Long.MAX_VALUE / 4;

    private final QueueRegistry registry;
    private final GlobalAdmissionGate gate;
    private final TimeSource time;

    private final AtomicLong ids = new AtomicLong();

    public AdmissionController(
            QueueRegistry registry,
            GlobalAdmissionGate gate,
            TimeSource time
    ) {
        this.registry = Objects.requireNonNull(
                registry,
                "registry"
        );

        this.gate = Objects.requireNonNull(
                gate,
                "gate"
        );

        this.time = Objects.requireNonNull(
                time,
                "time"
        );
    }

    /**
     * Submits a workload without a deadline.
     *
     * @param queueId target workload queue
     * @param task task to execute
     * @param <T> task result type
     * @return accepted submission or rejection result
     */
    public <T> Submission<T> submit(
            String queueId,
            WorkloadTask<T> task
    ) {
        return submit(queueId, task, null);
    }

    /**
     * Submits a workload with an optional relative deadline.
     *
     * <p>The deadline duration is measured from submission time using
     * the configured monotonic TimeSource.
     *
     * @param queueId target workload queue
     * @param task task to execute
     * @param deadline maximum time allowed before the task expires;
     *                 null means no deadline
     * @param <T> task result type
     * @return accepted submission or rejection result
     * @throws NullPointerException if queueId or task is null
     * @throws IllegalArgumentException if the queue does not exist,
     *         according to QueueRegistry.require()
     */
    public <T> Submission<T> submit(
            String queueId,
            WorkloadTask<T> task,
            Duration deadline
    ) {

        Objects.requireNonNull(queueId, "queueId");
        Objects.requireNonNull(task, "task");

        WorkloadQueue queue = registry.require(queueId);

        // Reject invalid deadlines before acquiring capacity.
        if (deadline != null &&
                (deadline.isZero() || deadline.isNegative())) {

            return Submission.rejected(
                    RejectionReason.INVALID_TASK
            );
        }

        // Fast shutdown check. The actual reservation operation
        // remains authoritative against concurrent shutdown.
        if (gate.isClosed()) {
            return Submission.rejected(
                    RejectionReason.SHUTDOWN
            );
        }

        // Reserve local and global admission capacity.
        WorkloadQueue.Reservation reservation =
                queue.tryReserve();

        if (reservation != WorkloadQueue.Reservation.RESERVED) {
            return Submission.rejected(
                    toReason(reservation)
            );
        }

        TaskRecord<T> record;

        try {

            long now = time.nanoTime();

            boolean hasDeadline = deadline != null;

            long deadlineNanos = hasDeadline
                    ? now + saturatedNanos(deadline)
                    : 0L;

            record = new TaskRecord<>(
                    ids.incrementAndGet(),
                    queue.id(),
                    task,
                    now,
                    hasDeadline,
                    deadlineNanos
            );

            // The task must be QUEUED before it becomes visible
            // to the dispatcher.
            if (!record.markQueued()) {
                throw new IllegalStateException(
                        "Could not transition task to QUEUED"
                );
            }

            // Successful enqueue transfers reservation ownership
            // to the queue.
            queue.enqueueReserved(record);

        } catch (RuntimeException | Error failure) {

            // Release the reservation if task creation or enqueueing
            // fails before successful publication.
            queue.abortReservation();

            throw failure;
        }

        return Submission.accepted(record);
    }

    /**
     * Offers a task that is waiting for a retry back to normal admission.
     *
     * <p>This is the <em>same</em> protocol a new submission uses: local and
     * global capacity are reserved with {@link WorkloadQueue#tryReserve()}, the
     * task becomes QUEUED, and it is published with
     * {@link WorkloadQueue#enqueueReserved(TaskRecord)}. There is no other path
     * from RETRY_WAIT to execution; the dispatcher, scheduler and concurrency
     * ledger then treat the task like any other queued task.
     *
     * <p>The task record (and therefore its id and future) is reused. Its
     * original deadline is not changed. If the deadline has already elapsed,
     * nothing is reserved and {@link ReadmissionResult#DEADLINE_EXPIRED} is
     * returned for the caller to act on. A refusal is reported, never retried
     * here, and never leaves a reservation behind.
     *
     * <p>Callers that must be atomic with shutdown (the runtime) invoke this
     * under the same lock that submissions use.
     *
     * @param record a task expected to be in RETRY_WAIT
     * @return what happened; only {@link ReadmissionResult#ADMITTED} changes the task
     */
    public ReadmissionResult readmit(TaskRecord<?> record) {

        Objects.requireNonNull(record, "record");

        if (record.status() != TaskStatus.RETRY_WAIT) {
            return ReadmissionResult.NOT_WAITING;
        }

        if (record.isExpired(time.nanoTime())) {
            return ReadmissionResult.DEADLINE_EXPIRED;
        }

        if (gate.isClosed()) {
            return ReadmissionResult.SHUTDOWN;
        }

        WorkloadQueue queue = registry.require(record.queueId());

        WorkloadQueue.Reservation reservation = queue.tryReserve();

        if (reservation != WorkloadQueue.Reservation.RESERVED) {
            return switch (reservation) {
                case QUEUE_FULL -> ReadmissionResult.QUEUE_FULL;
                case GLOBAL_LIMIT -> ReadmissionResult.GLOBAL_LIMIT;
                default -> ReadmissionResult.SHUTDOWN;
            };
        }

        boolean published = false;

        try {

            // Lost the race to cancellation: give the reservation back.
            if (!record.requeueForRetry(time.nanoTime())) {
                return ReadmissionResult.NOT_WAITING;
            }

            queue.enqueueReserved(record);
            published = true;

            return ReadmissionResult.ADMITTED;

        } finally {

            if (!published) {
                queue.abortReservation();
            }
        }
    }

    /**
     * Permanently stops admission.
     *
     * <p>Reservations acquired before shutdown may still be completed
     * or released. No new global reservations can succeed afterward.
     */
    public void shutdown() {
        gate.close();
    }

    /**
     * Returns whether admission is currently open.
     *
     * <p>This is a snapshot. A subsequent submission may still race
     * with shutdown; the reservation operation determines the outcome.
     *
     * @return true if admission is open
     */
    public boolean isAccepting() {
        return !gate.isClosed();
    }

    private static RejectionReason toReason(
            WorkloadQueue.Reservation reservation
    ) {

        return switch (reservation) {

            case QUEUE_FULL ->
                    RejectionReason.QUEUE_FULL;

            case GLOBAL_LIMIT ->
                    RejectionReason.GLOBAL_LIMIT;

            case CLOSED ->
                    RejectionReason.SHUTDOWN;

            case RESERVED ->
                    throw new IllegalArgumentException(
                            "RESERVED is not a rejection reason"
                    );
        };
    }

    /**
     * Converts a positive Duration into a bounded nanosecond value.
     *
     * <p>The bound keeps deadline comparisons safe when using
     * subtraction-based monotonic time arithmetic.
     */
    private static long saturatedNanos(Duration duration) {

        try {
            return Math.min(
                    duration.toNanos(),
                    MAX_DEADLINE_NANOS
            );

        } catch (ArithmeticException exception) {

            return MAX_DEADLINE_NANOS;
        }
    }

}
