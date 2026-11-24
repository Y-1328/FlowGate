
package io.flowgate.internal.dispatch;

import io.flowgate.api.TaskContext;
import io.flowgate.api.TaskStatus;
import io.flowgate.exception.RetryFailedException;
import io.flowgate.internal.ledger.ExecutionPermit;
import io.flowgate.internal.retry.RetryScheduler;
import io.flowgate.internal.task.TaskRecord;
import io.flowgate.internal.time.TimeSource;
import io.flowgate.policy.RetryPolicy;

import java.util.Objects;
import java.util.concurrent.TimeoutException;

/**
 * Executes one scheduled FlowGate task.
 *
 * <p>The execution permit is owned by this runnable and is always
 * released when execution finishes or is skipped.</p>
 *
 * <p>Running task deadlines are cooperative. The task receives
 * deadline information through TaskContext and is responsible
 * for checking it during execution.</p>
 *
 * <p>Retries: when the queue's {@link RetryPolicy} allows it, a failed attempt
 * moves the task RUNNING -> RETRY_WAIT and parks it in the
 * {@link RetryScheduler}; the worker thread then returns immediately and never
 * waits out a backoff. The permit is released exactly once, in the
 * {@code finally} of {@link #run()}, whichever way the attempt ends. A retry
 * never executes here: it re-enters normal admission and is dispatched like any
 * other task.</p>
 */
public final class TaskExecution implements Runnable {

    private final TaskRecord<?> record;
    private final ExecutionPermit permit;
    private final TimeSource time;
    private final RetryPolicy retryPolicy;
    private final RetryScheduler retryScheduler;

    /**
     * Creates an execution without retry support.
     */
    public TaskExecution(
            TaskRecord<?> record,
            ExecutionPermit permit,
            TimeSource time
    ) {
        this(record, permit, time, RetryPolicy.disabled(), null);
    }

    /**
     * @param retryPolicy policy of the task's queue
     * @param retryScheduler where retries are parked; null disables retries
     */
    public TaskExecution(
            TaskRecord<?> record,
            ExecutionPermit permit,
            TimeSource time,
            RetryPolicy retryPolicy,
            RetryScheduler retryScheduler
    ) {
        this.record = Objects.requireNonNull(record);
        this.permit = Objects.requireNonNull(permit);
        this.time = Objects.requireNonNull(time);
        this.retryPolicy = Objects.requireNonNull(retryPolicy);
        this.retryScheduler = retryScheduler;

        if (permit.queueIndex() < 0) {
            throw new IllegalArgumentException(
                    "Invalid permit queue index"
            );
        }
    }

    @Override
    public void run() {

        try {

            if (record.isExpired(time.nanoTime())) {
                expire();
                return;
            }

            if (!record.tryTransition(TaskStatus.RUNNING)) {
                return;
            }

            executeTask();

        } finally {
            permit.close();
        }
    }

    private void expire() {

        if (record.tryTransition(TaskStatus.EXPIRED)) {

            record.fail(
                    new TimeoutException(
                            "Task deadline exceeded before execution: "
                                    + record.id()
                    )
            );
        }
    }

    private void executeTask() {

        try {

            executeTyped(record);

        } catch (Throwable failure) {

            final boolean retryHandled;

            try {
                retryHandled = handledByRetry(failure);

            } catch (Throwable policyFailure) {

                // Preserve the original workload failure.
                // The policy failure is attached for diagnostics.
                if (policyFailure != failure) {
                    try {
                        failure.addSuppressed(policyFailure);
                    } catch (Throwable ignored) {
                        // Never leave the task stranded.
                    }
                }

                if (record.tryTransition(TaskStatus.FAILED)) {
                    record.fail(failure);
                }

                // Rethrow fatal policy errors only after terminalization.
                if (policyFailure instanceof Error error) {
                    throw error;
                }

                if (failure instanceof Error error) {
                    throw error;
                }

                return;
            }

            if (!retryHandled
                    && record.tryTransition(TaskStatus.FAILED)) {

                record.fail(failure);
            }

            // Preserve fatal workload errors.
            if (failure instanceof Error error) {
                throw error;
            }
        }
    }

    /**
     * Applies the retry policy to a failed attempt.
     *
     * @return true if retry handling took responsibility for the task
     */
    private boolean handledByRetry(Throwable failure) {

        if (retryScheduler == null
                || !retryPolicy.shouldRetry(failure)) {
            return false;
        }

        int attempt = record.attempt();

        if (attempt >= retryPolicy.maxAttempts()) {

            record.failRetry(new RetryFailedException(
                    RetryFailedException.Reason.ATTEMPTS_EXHAUSTED,
                    attempt,
                    "Task " + record.id()
                            + " failed after " + attempt
                            + " attempt(s); retries exhausted",
                    failure
            ));

            return true;
        }

        if (record.isExpired(time.nanoTime())) {
            return false;
        }

        long delayNanos = retryPolicy.backoffNanos(attempt);

        if (!record.beginRetryWait(failure)) {
            return true;
        }

        try {

            switch (retryScheduler.schedule(record, delayNanos)) {

                case SCHEDULED -> {
                    // Retry scheduler owns the task.
                }

                case CAPACITY_FULL ->
                        record.failRetry(new RetryFailedException(
                                RetryFailedException.Reason.RETRY_CAPACITY_FULL,
                                attempt,
                                "Task " + record.id()
                                        + " failed on attempt " + attempt
                                        + " and the bounded retry capacity is full",
                                failure
                        ));

                case CLOSED -> record.cancel();
            }

        } catch (RuntimeException schedulingFailure) {

            RetryFailedException wrapper = new RetryFailedException(
                    RetryFailedException.Reason.SCHEDULING_FAILED,
                    attempt,
                    "Task " + record.id()
                            + " could not be scheduled for retry",
                    failure
            );

            wrapper.addSuppressed(schedulingFailure);
            record.failRetry(wrapper);
        }

        return true;
    }

    private <T> void executeTyped(TaskRecord<T> taskRecord)
            throws Exception {

        TaskContext context = new TaskContext() {

            @Override
            public boolean isCancellationRequested() {
                return taskRecord.isCancellationRequested();
            }

            @Override
            public boolean hasDeadline() {
                return taskRecord.hasDeadline();
            }

            @Override
            public boolean isDeadlineExceeded() {
                return taskRecord.isDeadlineExceeded(
                        time.nanoTime()
                );
            }

            @Override
            public long remainingTimeNanos() {
                return taskRecord.remainingTimeNanos(
                        time.nanoTime()
                );
            }

            @Override
            public int attempt() {
                return taskRecord.attempt();
            }
        };

        T result = taskRecord.task().run(context);

        if (taskRecord.tryTransition(TaskStatus.COMPLETED)) {
            taskRecord.complete(result);
        }
    }
}
