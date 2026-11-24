package io.flowgate.api;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Result of submitting a task to FlowGate.
 *
 * Represents either:
 * 1. An accepted task with a TaskHandle.
 * 2. A rejected submission with a RejectionReason.
 */
public final class Submission<T> {

    private static final Submission<?>[] REJECTIONS =
            new Submission<?>[RejectionReason.values().length];

    static {
        for (RejectionReason reason : RejectionReason.values()) {
            REJECTIONS[reason.ordinal()] =
                    new Submission<>(null, reason);
        }
    }

    private final TaskHandle<T> handle;
    private final RejectionReason reason;

    private Submission(TaskHandle<T> handle, RejectionReason reason) {
        this.handle = handle;
        this.reason = reason;
    }

    // ---------- Factory methods ----------

    public static <T> Submission<T> accepted(TaskHandle<T> handle) {
        return new Submission<>(
                Objects.requireNonNull(handle, "handle"),
                null
        );
    }

    @SuppressWarnings("unchecked")
    public static <T> Submission<T> rejected(RejectionReason reason) {
        return (Submission<T>) REJECTIONS[
                Objects.requireNonNull(reason, "reason").ordinal()
                ];
    }

    // ---------- Admission status ----------

    public AdmissionOutcome outcome() {
        return isAccepted()
                ? AdmissionOutcome.ACCEPTED
                : AdmissionOutcome.REJECTED;
    }

    public boolean isAccepted() {
        return handle != null;
    }

    public boolean isRejected() {
        return handle == null;
    }

    // ---------- Existing API ----------

    /**
     * Returns the TaskHandle for an accepted submission.
     *
     * @throws IllegalStateException if the submission was rejected
     */
    public TaskHandle<T> handle() {
        if (handle == null) {
            throw new IllegalStateException(
                    "Submission was rejected: " + reason
            );
        }

        return handle;
    }

    /**
     * Returns the rejection reason.
     *
     * @throws IllegalStateException if the submission was accepted
     */
    public RejectionReason rejectionReason() {
        if (reason == null) {
            throw new IllegalStateException(
                    "Submission was accepted"
            );
        }

        return reason;
    }

    // ---------- Convenience API ----------

    /**
     * Returns the ID of the accepted task.
     */
    public long id() {
        return handle().id();
    }

    /**
     * Returns the queue this task was submitted to.
     */
    public String queueId() {
        return handle().queueId();
    }

    /**
     * Returns the current task status.
     */
    public TaskStatus status() {
        return handle().status();
    }

    /**
     * Returns the task's completion future.
     */
    public CompletableFuture<T> future() {
        return handle().future();
    }

    /**
     * Attempts to cancel the accepted task.
     */
    public boolean cancel() {
        return handle().cancel();
    }

    // ---------- Object representation ----------

    @Override
    public String toString() {
        return handle != null
                ? "Submission[ACCEPTED id=" + handle.id() + "]"
                : "Submission[REJECTED " + reason + "]";
    }
}