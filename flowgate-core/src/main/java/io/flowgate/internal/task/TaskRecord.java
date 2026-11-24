
package io.flowgate.internal.task;

import io.flowgate.api.TaskHandle;
import io.flowgate.api.TaskStatus;
import io.flowgate.api.WorkloadTask;
import io.flowgate.internal.lifecycle.TaskStateMachine;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Internal representation of an accepted task.
 *
 * TaskStateMachine is the single source of truth for lifecycle state.
 *
 * <p>One record, one id and one future represent a logical task across all of
 * its retry attempts. Only the attempt counter, the queue-entry timestamp and
 * the most recent failure change between attempts.
 */
public final class TaskRecord<T> implements TaskHandle<T> {

    private final long id;
    private final String queueId;
    private final WorkloadTask<T> task;

    /** When the task last entered its queue; refreshed on each re-admission. */
    private volatile long enqueueNanos;
    private final boolean hasDeadline;
    private final long deadlineNanos;

    private final TaskStateMachine stateMachine =
            new TaskStateMachine();

    private final CompletableFuture<T> future =
            new CompletableFuture<>();

    private final AtomicBoolean cancellationRequested =
            new AtomicBoolean(false);

    /** 1-based number of the current (or next scheduled) attempt. */
    private final AtomicInteger attempt = new AtomicInteger(1);

    /**
     * Failure of the most recent attempt while a retry is pending. Holds at
     * most one throwable and is cleared on every terminal transition.
     */
    private volatile Throwable lastFailure;

    public TaskRecord(
            long id,
            String queueId,
            WorkloadTask<T> task,
            long enqueueNanos,
            boolean hasDeadline,
            long deadlineNanos
    ) {
        this.id = id;
        this.queueId = Objects.requireNonNull(queueId);
        this.task = Objects.requireNonNull(task);
        this.enqueueNanos = enqueueNanos;
        this.hasDeadline = hasDeadline;
        this.deadlineNanos = deadlineNanos;
    }

    @Override
    public long id() {
        return id;
    }

    @Override
    public String queueId() {
        return queueId;
    }

    @Override
    public TaskStatus status() {
        return stateMachine.status();
    }

    /**
     * Transitions CREATED -> QUEUED.
     */
    public boolean markQueued() {
        return stateMachine.tryTransition(TaskStatus.QUEUED);
    }

    /**
     * Attempts a lifecycle transition.
     */
    public boolean tryTransition(TaskStatus target) {
        return stateMachine.tryTransition(target);
    }

    /**
     * Attempts to cancel this task.
     */
    @Override
    public boolean cancel() {

        if (!stateMachine.tryTransition(TaskStatus.CANCELLED)) {
            return false;
        }

        cancellationRequested.set(true);
        lastFailure = null;

        future.completeExceptionally(
                new CancellationException("Task cancelled: " + id)
        );

        return true;
    }

    /**
     * Returns the 1-based attempt number: the attempt currently executing, or,
     * while the task waits for a retry, the attempt that last failed.
     */
    public int attempt() {
        return attempt.get();
    }

    /**
     * Returns the failure of the most recent attempt if a retry is pending;
     * null otherwise.
     */
    public Throwable lastFailure() {
        return lastFailure;
    }

    /**
     * Transitions RUNNING -> RETRY_WAIT, remembering why the attempt failed.
     *
     * @return false if another actor (cancellation) reached a terminal state first
     */
    public boolean beginRetryWait(Throwable failure) {

        Objects.requireNonNull(failure, "failure");

        lastFailure = failure;

        if (stateMachine.tryTransition(TaskStatus.RETRY_WAIT)) {
            return true;
        }

        lastFailure = null;
        return false;
    }

    /**
     * Transitions RETRY_WAIT -> QUEUED for the next attempt, increments the
     * attempt number and restarts queue-wait accounting (priority aging counts
     * from this re-admission, not from the original submission). The deadline is
     * untouched.
     *
     * <p>Must be called before the record is published to its queue.
     *
     * @return false if the task is no longer in RETRY_WAIT
     */
    public boolean requeueForRetry(long nowNanos) {

        if (!stateMachine.tryTransition(TaskStatus.QUEUED)) {
            return false;
        }

        enqueueNanos = nowNanos;
        attempt.incrementAndGet();

        return true;
    }

    /**
     * Expires a task whose deadline passed while it waited for a retry.
     * The future fails with a {@link TimeoutException} whose cause is the last
     * attempt's failure.
     *
     * @return true if this call performed the transition
     */
    public boolean expireDuringRetry() {

        if (!stateMachine.tryTransition(TaskStatus.EXPIRED)) {
            return false;
        }

        TimeoutException timeout = new TimeoutException(
                "Task deadline exceeded during retry backoff: " + id
        );

        Throwable cause = lastFailure;

        if (cause != null) {
            timeout.initCause(cause);
        }

        lastFailure = null;
        future.completeExceptionally(timeout);

        return true;
    }

    /**
     * Fails the task because retrying cannot continue. Legal from RUNNING and
     * RETRY_WAIT.
     *
     * @return true if this call performed the transition and completed the future
     */
    public boolean failRetry(Throwable failure) {

        Objects.requireNonNull(failure, "failure");

        if (!stateMachine.tryTransition(TaskStatus.FAILED)) {
            return false;
        }

        lastFailure = null;
        future.completeExceptionally(failure);

        return true;
    }

    /**
     * Completes the task's result.
     */
    public void complete(T result) {
        lastFailure = null;
        future.complete(result);
    }

    /**
     * Completes the task exceptionally.
     */
    public void fail(Throwable throwable) {
        Objects.requireNonNull(throwable);
        lastFailure = null;
        future.completeExceptionally(throwable);
    }

    /**
     * Returns a dependent future so callers cannot directly complete
     * the framework's internal future.
     */
    @Override
    public CompletableFuture<T> future() {
        return future.copy();
    }

    public WorkloadTask<T> task() {
        return task;
    }

    public long enqueueNanos() {
        return enqueueNanos;
    }

    public boolean isCancellationRequested() {
        return cancellationRequested.get();
    }

    /**
     * Checks the relative deadline using nanoTime arithmetic.
     */
    public boolean isExpired(long nowNanos) {
        return hasDeadline && nowNanos - deadlineNanos >= 0;
    }

    /**
     * Returns whether this task has a deadline.
     */
    public boolean hasDeadline() {
        return hasDeadline;
    }

    /**
     * Returns whether the deadline has elapsed at the supplied time.
     */
    public boolean isDeadlineExceeded(long nowNanos) {
        return isExpired(nowNanos);
    }

    /**
     * Returns remaining deadline time in nanoseconds.
     *
     * @return remaining nanoseconds, zero if expired,
     *         or Long.MAX_VALUE if no deadline exists
     */
    public long remainingTimeNanos(long nowNanos) {

        if (!hasDeadline) {
            return Long.MAX_VALUE;
        }

        long remaining = deadlineNanos - nowNanos;

        return Math.max(0L, remaining);
    }

}
