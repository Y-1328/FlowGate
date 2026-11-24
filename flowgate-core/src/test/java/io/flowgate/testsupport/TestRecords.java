package io.flowgate.testsupport;

import io.flowgate.api.TaskStatus;
import io.flowgate.api.WorkloadTask;
import io.flowgate.internal.task.TaskRecord;

import java.io.IOException;

/** Builds task records in specific lifecycle states without going through admission. */
public final class TestRecords {

    private TestRecords() {
    }

    public static <T> TaskRecord<T> queued(
            long id, String queueId, WorkloadTask<T> task,
            long enqueueNanos, boolean hasDeadline, long deadlineNanos
    ) {
        TaskRecord<T> record = new TaskRecord<>(
                id, queueId, task, enqueueNanos, hasDeadline, deadlineNanos
        );

        if (!record.markQueued()) {
            throw new AssertionError("markQueued failed");
        }

        return record;
    }

    public static <T> TaskRecord<T> running(
            long id, String queueId, WorkloadTask<T> task,
            long enqueueNanos, boolean hasDeadline, long deadlineNanos
    ) {
        TaskRecord<T> record =
                queued(id, queueId, task, enqueueNanos, hasDeadline, deadlineNanos);

        if (!record.tryTransition(TaskStatus.SCHEDULED)
                || !record.tryTransition(TaskStatus.RUNNING)) {
            throw new AssertionError("could not reach RUNNING");
        }

        return record;
    }

    /** A task whose first attempt failed with an IOException and now waits for a retry. */
    public static TaskRecord<String> retryWaiting(
            long id, String queueId, boolean hasDeadline, long deadlineNanos
    ) {
        TaskRecord<String> record =
                running(id, queueId, ctx -> "ok", 0L, hasDeadline, deadlineNanos);

        if (!record.beginRetryWait(new IOException("attempt 1 failed"))) {
            throw new AssertionError("could not reach RETRY_WAIT");
        }

        return record;
    }

    public static TaskRecord<String> retryWaiting(long id) {
        return retryWaiting(id, "q", false, 0L);
    }
}
