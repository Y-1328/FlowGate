package io.flowgate.internal.task;

import io.flowgate.api.TaskStatus;
import io.flowgate.testsupport.Concurrently;
import io.flowgate.testsupport.TestRecords;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class TaskRecordRetryTest {

    @Test
    void firstAttemptIsNumberOneAndThereIsNoFailureYet() {
        TaskRecord<String> record = TestRecords.queued(1, "q", c -> "x", 10, false, 0);

        assertEquals(1, record.attempt());
        assertNull(record.lastFailure());
    }

    @Test
    void beginRetryWaitRecordsTheFailureAndMovesToRetryWait() {
        TaskRecord<String> record = TestRecords.running(1, "q", c -> "x", 0, false, 0);
        IOException failure = new IOException("boom");

        assertTrue(record.beginRetryWait(failure));

        assertEquals(TaskStatus.RETRY_WAIT, record.status());
        assertSame(failure, record.lastFailure());
        assertFalse(record.future().isDone(), "intermediate failures must not complete the future");
    }

    @Test
    void beginRetryWaitIsOnlyLegalFromRunning() {
        TaskRecord<String> queued = TestRecords.queued(1, "q", c -> "x", 0, false, 0);

        assertFalse(queued.beginRetryWait(new IOException()));
        assertEquals(TaskStatus.QUEUED, queued.status());
        assertNull(queued.lastFailure(), "a rejected transition must not retain the failure");
    }

    @Test
    void beginRetryWaitLosesToCancellation() {
        TaskRecord<String> record = TestRecords.running(1, "q", c -> "x", 0, false, 0);

        assertTrue(record.cancel());
        assertFalse(record.beginRetryWait(new IOException()));

        assertEquals(TaskStatus.CANCELLED, record.status());
        assertNull(record.lastFailure());
    }

    @Test
    void requeueIncrementsTheAttemptAndRestartsQueueWaitAccounting() {
        TaskRecord<String> record = TestRecords.retryWaiting(1, "q", true, 5_000);
        long originalEnqueue = record.enqueueNanos();

        assertTrue(record.requeueForRetry(900));

        assertEquals(TaskStatus.QUEUED, record.status());
        assertEquals(2, record.attempt());
        assertEquals(900, record.enqueueNanos());
        assertNotEquals(originalEnqueue, record.enqueueNanos());
    }

    @Test
    void requeueNeverChangesTheOriginalDeadline() {
        TaskRecord<String> record = TestRecords.retryWaiting(1, "q", true, 5_000);

        long remainingBefore = record.remainingTimeNanos(1_000);
        assertTrue(record.requeueForRetry(3_000));

        assertTrue(record.hasDeadline());
        assertEquals(remainingBefore, record.remainingTimeNanos(1_000));
        assertFalse(record.isExpired(4_999));
        assertTrue(record.isExpired(5_000));
    }

    @Test
    void requeueIsOnlyLegalFromRetryWait() {
        TaskRecord<String> running = TestRecords.running(1, "q", c -> "x", 0, false, 0);

        assertFalse(running.requeueForRetry(5));
        assertEquals(1, running.attempt());
        assertEquals(TaskStatus.RUNNING, running.status());
    }

    @Test
    void secondRequeueFailsSoAnAttemptIsNeverCountedTwice() {
        TaskRecord<String> record = TestRecords.retryWaiting(1);

        assertTrue(record.requeueForRetry(10));
        assertFalse(record.requeueForRetry(20));

        assertEquals(2, record.attempt());
        assertEquals(10, record.enqueueNanos());
    }

    @Test
    void cancellationInRetryWaitIsTerminalAndClearsTheFailure() {
        TaskRecord<String> record = TestRecords.retryWaiting(1);

        assertTrue(record.cancel());

        assertEquals(TaskStatus.CANCELLED, record.status());
        assertTrue(record.isCancellationRequested());
        assertNull(record.lastFailure());
        assertFalse(record.requeueForRetry(5), "a cancelled task cannot be resurrected");

        CompletionException e = assertThrows(CompletionException.class, () -> record.future().join());
        assertInstanceOf(CancellationException.class, e.getCause());
    }

    @Test
    void expireDuringRetryFailsTheFutureWithATimeoutCarryingTheLastFailure() {
        TaskRecord<String> record = TestRecords.retryWaiting(7);
        Throwable last = record.lastFailure();

        assertTrue(record.expireDuringRetry());

        assertEquals(TaskStatus.EXPIRED, record.status());
        assertNull(record.lastFailure());

        CompletionException e = assertThrows(CompletionException.class, () -> record.future().join());
        assertInstanceOf(TimeoutException.class, e.getCause());
        assertSame(last, e.getCause().getCause());
        assertTrue(e.getCause().getMessage().contains("7"));
    }

    @Test
    void failRetryIsLegalFromRunningAndRetryWaitAndNotFromQueued() {
        TaskRecord<String> running = TestRecords.running(1, "q", c -> "x", 0, false, 0);
        assertTrue(running.failRetry(new IllegalStateException("a")));
        assertEquals(TaskStatus.FAILED, running.status());

        TaskRecord<String> waiting = TestRecords.retryWaiting(2);
        assertTrue(waiting.failRetry(new IllegalStateException("b")));
        assertEquals(TaskStatus.FAILED, waiting.status());

        TaskRecord<String> queued = TestRecords.queued(3, "q", c -> "x", 0, false, 0);
        assertFalse(queued.failRetry(new IllegalStateException("c")));
        assertFalse(queued.future().isDone());
    }

    @Test
    void terminalStatesAreImmutableAfterRetry() {
        TaskRecord<String> record = TestRecords.retryWaiting(1);

        assertTrue(record.failRetry(new IllegalStateException("final")));

        assertFalse(record.requeueForRetry(1));
        assertFalse(record.expireDuringRetry());
        assertFalse(record.cancel());
        assertFalse(record.beginRetryWait(new IOException()));
        assertEquals(TaskStatus.FAILED, record.status());
    }

    @Test
    void completionAndFailureClearTheRetainedFailure() {
        TaskRecord<String> completed = TestRecords.retryWaiting(1);
        assertTrue(completed.requeueForRetry(1));
        completed.complete("done");
        assertNull(completed.lastFailure());

        TaskRecord<String> failed = TestRecords.retryWaiting(2);
        failed.fail(new IllegalStateException());
        assertNull(failed.lastFailure());
    }

    @Test
    void failureRetentionIsBoundedToOneThrowable() {
        TaskRecord<String> record = TestRecords.running(1, "q", c -> "x", 0, false, 0);

        for (int i = 1; i <= 50; i++) {
            IOException failure = new IOException("attempt " + i);

            assertTrue(record.beginRetryWait(failure));
            assertSame(failure, record.lastFailure());
            assertTrue(record.requeueForRetry(i));
            assertTrue(record.tryTransition(TaskStatus.SCHEDULED));
            assertTrue(record.tryTransition(TaskStatus.RUNNING));
        }

        assertEquals(51, record.attempt());
    }

    @Test
    void oneIdAndOneFutureSpanEveryAttempt() {
        TaskRecord<String> record = TestRecords.running(42, "q", c -> "x", 0, false, 0);

        for (int i = 0; i < 5; i++) {
            assertTrue(record.beginRetryWait(new IOException()));
            assertTrue(record.requeueForRetry(i));
            assertTrue(record.tryTransition(TaskStatus.SCHEDULED));
            assertTrue(record.tryTransition(TaskStatus.RUNNING));

            assertEquals(42, record.id());
            assertFalse(record.future().isDone());
        }

        record.tryTransition(TaskStatus.COMPLETED);
        record.complete("final");

        assertEquals("final", record.future().join());
    }

    @Test
    void exactlyOneConcurrentRequeueWins() throws Exception {
        for (int round = 0; round < 300; round++) {
            TaskRecord<String> record = TestRecords.retryWaiting(round);
            AtomicInteger winners = new AtomicInteger();

            Concurrently.run(8, i -> {
                if (record.requeueForRetry(i)) {
                    winners.incrementAndGet();
                }
            });

            assertEquals(1, winners.get());
            assertEquals(2, record.attempt(), "the attempt must be counted exactly once");
        }
    }

    @Test
    void cancellationRacingWithRequeueAlwaysEndsCancelledAndNeverResurrects() throws Exception {
        for (int round = 0; round < 500; round++) {
            TaskRecord<String> record = TestRecords.retryWaiting(round);
            AtomicInteger requeued = new AtomicInteger();
            AtomicInteger cancelled = new AtomicInteger();

            Concurrently.run(2, i -> {
                if (i == 0) {
                    if (record.requeueForRetry(1)) {
                        requeued.incrementAndGet();
                    }
                } else if (record.cancel()) {
                    cancelled.incrementAndGet();
                }
            });

            // Cancel is legal from RETRY_WAIT and from QUEUED, so it always wins
            // eventually; requeue wins only if it ran first. Either way the task
            // ends CANCELLED, and the attempt is counted only if the requeue won.
            assertEquals(1, cancelled.get());
            assertEquals(TaskStatus.CANCELLED, record.status());
            assertEquals(1 + requeued.get(), record.attempt());
            assertFalse(record.requeueForRetry(9), "terminal state must stay terminal");
        }
    }
}
