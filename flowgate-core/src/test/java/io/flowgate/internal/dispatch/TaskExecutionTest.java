
package io.flowgate.internal.dispatch;

import io.flowgate.api.Priority;
import io.flowgate.api.TaskStatus;
import io.flowgate.config.QueueConfig;
import io.flowgate.internal.ledger.ConcurrencyLedger;
import io.flowgate.internal.ledger.ExecutionPermit;
import io.flowgate.internal.queue.GlobalAdmissionGate;
import io.flowgate.internal.queue.QueueRegistry;
import io.flowgate.internal.task.TaskRecord;
import io.flowgate.internal.time.TimeSource;
import org.junit.jupiter.api.Test;
import java.util.concurrent.atomic.AtomicLong;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class TaskExecutionTest {

    private final TimeSource time = () -> 100L;

    private final QueueConfig config = new QueueConfig(
            "default",
            Priority.NORMAL,
            1,
            10,
            1,
            1,
            null
    );

    private ExecutionPermit acquirePermit() {
        GlobalAdmissionGate gate = new GlobalAdmissionGate(10);
        QueueRegistry registry = new QueueRegistry(
                List.of(config),
                gate
        );

        ConcurrencyLedger ledger = new ConcurrencyLedger(1, registry);

        ExecutionPermit permit = ledger.tryAcquire(
                0,
                new int[]{1}
        );

        assertNotNull(permit);
        return permit;
    }

    private <T> TaskRecord<T> scheduledRecord(
            io.flowgate.api.WorkloadTask<T> task,
            boolean hasDeadline,
            long deadlineNanos
    ) {
        TaskRecord<T> record = new TaskRecord<>(
                1,
                "default",
                task,
                0L,
                hasDeadline,
                deadlineNanos
        );

        assertTrue(record.markQueued());
        assertTrue(record.tryTransition(TaskStatus.SCHEDULED));

        return record;
    }

    @Test
    void shouldExecuteAndCompleteTask() {
        TaskRecord<String> record = scheduledRecord(
                context -> "success",
                false,
                0L
        );

        ExecutionPermit permit = acquirePermit();

        new TaskExecution(record, permit, time).run();

        assertEquals(TaskStatus.COMPLETED, record.status());
        assertEquals("success", record.future().join());
        assertTrue(permit.isReleased());
    }

    @Test
    void shouldFailTaskWhenExecutionThrows() {
        TaskRecord<String> record = scheduledRecord(
                context -> {
                    throw new IllegalStateException("task failed");
                },
                false,
                0L
        );

        ExecutionPermit permit = acquirePermit();

        new TaskExecution(record, permit, time).run();

        assertEquals(TaskStatus.FAILED, record.status());

        CompletionException exception = assertThrows(
                CompletionException.class,
                () -> record.future().join()
        );

        assertInstanceOf(
                IllegalStateException.class,
                exception.getCause()
        );

        assertTrue(permit.isReleased());
    }

    @Test
    void shouldExpireTaskBeforeExecution() {
        AtomicBoolean executed = new AtomicBoolean(false);

        TaskRecord<String> record = scheduledRecord(
                context -> {
                    executed.set(true);
                    return "unexpected";
                },
                true,
                50L
        );

        ExecutionPermit permit = acquirePermit();

        new TaskExecution(record, permit, time).run();

        assertEquals(TaskStatus.EXPIRED, record.status());
        assertFalse(executed.get());
        assertTrue(permit.isReleased());

        CompletionException exception = assertThrows(
                CompletionException.class,
                () -> record.future().join()
        );

        assertInstanceOf(
                java.util.concurrent.TimeoutException.class,
                exception.getCause()
        );
    }

    @Test
    void shouldSkipCancelledTaskAndReleasePermit() {
        AtomicBoolean executed = new AtomicBoolean(false);

        TaskRecord<String> record = scheduledRecord(
                context -> {
                    executed.set(true);
                    return "unexpected";
                },
                false,
                0L
        );

        assertTrue(record.cancel());

        ExecutionPermit permit = acquirePermit();

        new TaskExecution(record, permit, time).run();

        assertEquals(TaskStatus.CANCELLED, record.status());
        assertFalse(executed.get());
        assertTrue(permit.isReleased());
    }

    @Test
    void shouldExposeCancellationThroughTaskContext() {
        AtomicBoolean cancellationObserved = new AtomicBoolean(false);

        TaskRecord<String> record = scheduledRecord(
                context -> {
                    cancellationObserved.set(
                            context.isCancellationRequested()
                    );
                    return "finished";
                },
                false,
                0L
        );

        ExecutionPermit permit = acquirePermit();

        new TaskExecution(record, permit, time).run();

        assertFalse(cancellationObserved.get());
        assertEquals(TaskStatus.COMPLETED, record.status());
    }

    @Test
    void shouldExposeDeadlineInformationThroughTaskContext() {

        AtomicBoolean hasDeadline = new AtomicBoolean();
        AtomicBoolean deadlineExceeded = new AtomicBoolean();
        AtomicLong remainingTime = new AtomicLong(-1L);

        TaskRecord<String> record = scheduledRecord(
                context -> {
                    hasDeadline.set(context.hasDeadline());
                    deadlineExceeded.set(context.isDeadlineExceeded());
                    remainingTime.set(context.remainingTimeNanos());
                    return "success";
                },
                true,
                200L
        );

        ExecutionPermit permit = acquirePermit();

        new TaskExecution(record, permit, time).run();

        assertTrue(hasDeadline.get());
        assertFalse(deadlineExceeded.get());
        assertEquals(100L, remainingTime.get());

        assertEquals(TaskStatus.COMPLETED, record.status());
        assertTrue(permit.isReleased());
    }

    @Test
    void shouldExposeDeadlineExpirationDuringRunningExecution() {

        AtomicBoolean deadlineExceeded = new AtomicBoolean(false);
        AtomicLong remainingTime = new AtomicLong(-1L);

        AtomicInteger clockCalls = new AtomicInteger();

        TimeSource advancingTime = () ->
                clockCalls.incrementAndGet() == 1 ? 100L : 250L;

        TaskRecord<String> record = scheduledRecord(
                context -> {
                    deadlineExceeded.set(context.isDeadlineExceeded());
                    remainingTime.set(context.remainingTimeNanos());
                    return "completed cooperatively";
                },
                true,
                200L
        );

        ExecutionPermit permit = acquirePermit();

        new TaskExecution(record, permit, advancingTime).run();

        assertTrue(deadlineExceeded.get());
        assertEquals(0L, remainingTime.get());

        // Expiration is cooperative after execution starts.
        assertEquals(TaskStatus.COMPLETED, record.status());
        assertTrue(permit.isReleased());
    }

    @Test
    void shouldReportNoDeadlineCorrectly() {

        AtomicBoolean hasDeadline = new AtomicBoolean(true);
        AtomicBoolean deadlineExceeded = new AtomicBoolean(true);
        AtomicLong remainingTime = new AtomicLong();

        TaskRecord<String> record = scheduledRecord(
                context -> {
                    hasDeadline.set(context.hasDeadline());
                    deadlineExceeded.set(context.isDeadlineExceeded());
                    remainingTime.set(context.remainingTimeNanos());
                    return "success";
                },
                false,
                0L
        );

        ExecutionPermit permit = acquirePermit();

        new TaskExecution(record, permit, time).run();

        assertFalse(hasDeadline.get());
        assertFalse(deadlineExceeded.get());
        assertEquals(Long.MAX_VALUE, remainingTime.get());

        assertEquals(TaskStatus.COMPLETED, record.status());
        assertTrue(permit.isReleased());
    }

}
