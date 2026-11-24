
package io.flowgate.internal.dispatch;

import io.flowgate.api.Priority;
import io.flowgate.api.TaskStatus;
import io.flowgate.api.WorkloadTask;
import io.flowgate.config.QueueConfig;
import io.flowgate.exception.RetryFailedException;
import io.flowgate.internal.admission.ReadmissionResult;
import io.flowgate.internal.ledger.ConcurrencyLedger;
import io.flowgate.internal.ledger.ExecutionPermit;
import io.flowgate.internal.queue.GlobalAdmissionGate;
import io.flowgate.internal.queue.QueueRegistry;
import io.flowgate.internal.retry.RetryScheduler;
import io.flowgate.internal.task.TaskRecord;
import io.flowgate.policy.RetryPolicy;
import io.flowgate.testsupport.FakeRetryTimer;
import io.flowgate.testsupport.ManualTimeSource;
import io.flowgate.testsupport.TestRecords;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** The worker-side retry decision, with a fake timer and a manual clock. */
class TaskExecutionRetryTest {

    private static final long MS = 1_000_000L;

    private final ManualTimeSource clock = new ManualTimeSource(0L);
    private final FakeRetryTimer timer = new FakeRetryTimer();
    private final List<TaskRecord<?>> readmitted = new ArrayList<>();

    private final RetryPolicy policy = RetryPolicy.builder()
            .maxAttempts(3)
            .retryOn(IOException.class)
            .initialBackoff(Duration.ofMillis(100))
            .maxBackoff(Duration.ofSeconds(10))
            .multiplier(2.0)
            .build();

    private final ConcurrencyLedger ledger;
    private RetryScheduler scheduler;

    TaskExecutionRetryTest() {
        QueueRegistry registry = new QueueRegistry(
                List.of(new QueueConfig("q", Priority.NORMAL, 1, 10, 1, 1, null)),
                new GlobalAdmissionGate(10)
        );

        ledger = new ConcurrencyLedger(1, registry);
        scheduler = newScheduler(10);
    }

    private RetryScheduler newScheduler(int capacity) {
        return new RetryScheduler(clock, capacity, record -> {
            readmitted.add(record);
            return ReadmissionResult.ADMITTED;
        }, timer);
    }

    private <T> TaskRecord<T> scheduled(
            WorkloadTask<T> task, boolean hasDeadline, long deadlineNanos
    ) {
        TaskRecord<T> record = TestRecords.queued(1, "q", task, 0, hasDeadline, deadlineNanos);
        assertTrue(record.tryTransition(TaskStatus.SCHEDULED));
        return record;
    }

    private <T> TaskRecord<T> scheduled(WorkloadTask<T> task) {
        return scheduled(task, false, 0);
    }

    private void run(TaskRecord<?> record, RetryPolicy retryPolicy, RetryScheduler retryScheduler) {
        ExecutionPermit permit = ledger.tryAcquire(0, new int[]{1});
        assertNotNull(permit, "test fixture must be able to acquire a permit");

        try {
            new TaskExecution(record, permit, clock, retryPolicy, retryScheduler).run();
        } finally {
            assertEquals(0, ledger.activeTotal(), "the permit must be released exactly once");
        }
    }

    private void run(TaskRecord<?> record) {
        run(record, policy, scheduler);
    }

    private static Throwable causeOf(TaskRecord<?> record) {
        return assertThrows(CompletionException.class, () -> record.future().join()).getCause();
    }

    /**
     * Lets the backoff elapse (the scheduler consumes the parked entry, as it
     * would in production) and then performs the re-admission and scheduling
     * transitions that the runtime would perform for the next attempt.
     */
    private void nextAttempt(TaskRecord<?> record) {
        clock.advance(10_000 * MS);
        scheduler.runDue();
        assertTrue(record.requeueForRetry(clock.nanoTime()));
        assertTrue(record.tryTransition(TaskStatus.SCHEDULED));
    }

    // ---- no retry configured ------------------------------------------------

    @Test
    void withoutRetriesAFailureIsFinalExactlyAsBeforeM5() {
        IOException failure = new IOException("boom");
        TaskRecord<String> record = scheduled(c -> { throw failure; });

        run(record, RetryPolicy.disabled(), scheduler);

        assertEquals(TaskStatus.FAILED, record.status());
        assertSame(failure, causeOf(record));
        assertEquals(0, scheduler.pending());
    }

    @Test
    void aNullSchedulerDisablesRetriesEvenWithAnEnabledPolicy() {
        IOException failure = new IOException("boom");
        TaskRecord<String> record = scheduled(c -> { throw failure; });

        run(record, policy, null);

        assertEquals(TaskStatus.FAILED, record.status());
        assertSame(failure, causeOf(record));
    }

    @Test
    void theThreeArgumentConstructorStillWorksAndNeverRetries() {
        TaskRecord<String> record = scheduled(c -> { throw new IOException("x"); });
        ExecutionPermit permit = ledger.tryAcquire(0, new int[]{1});

        new TaskExecution(record, permit, clock).run();

        assertEquals(TaskStatus.FAILED, record.status());
        assertEquals(0, ledger.activeTotal());
    }

    @Test
    void aSuccessfulFirstAttemptIsUnaffectedByRetryConfiguration() {
        TaskRecord<String> record = scheduled(c -> "fine");

        run(record);

        assertEquals(TaskStatus.COMPLETED, record.status());
        assertEquals("fine", record.future().join());
        assertEquals(0, scheduler.pending());
    }

    // ---- retry decision --------------------------------------------------------

    @Test
    void aRetryableFailureParksTheTaskWithoutCompletingTheFutureAndReleasesThePermit() {
        IOException failure = new IOException("transient");
        TaskRecord<String> record = scheduled(c -> { throw failure; });

        run(record);

        assertEquals(TaskStatus.RETRY_WAIT, record.status());
        assertFalse(record.future().isDone(), "intermediate failure must not complete the future");
        assertSame(failure, record.lastFailure());
        assertEquals(1, record.attempt());
        assertEquals(1, scheduler.pending());
        assertEquals(100 * MS, timer.last().delayNanos);
    }

    @Test
    void backoffGrowsWithEachFailedAttempt() {
        TaskRecord<String> record = scheduled(c -> { throw new IOException(); });

        run(record);
        assertEquals(100 * MS, timer.last().delayNanos);

        nextAttempt(record);
        run(record);
        assertEquals(TaskStatus.RETRY_WAIT, record.status());
        assertEquals(2, record.attempt());
        assertEquals(200 * MS, timer.last().delayNanos);
    }

    @Test
    void theWorkerReturnsImmediatelyAndDoesNotWaitForTheBackoff() {
        RetryPolicy slow = RetryPolicy.builder()
                .maxAttempts(3)
                .retryOn(IOException.class)
                .initialBackoff(Duration.ofSeconds(5))
                .maxBackoff(Duration.ofSeconds(60))
                .build();

        TaskRecord<String> record = scheduled(c -> { throw new IOException(); });

        long before = System.nanoTime();
        run(record, slow, scheduler);
        long elapsed = System.nanoTime() - before;

        assertTrue(elapsed < Duration.ofSeconds(2).toNanos(), "worker must not sleep: " + elapsed);
        assertEquals(TaskStatus.RETRY_WAIT, record.status());
        assertEquals(5_000 * MS, timer.last().delayNanos);
    }

    @Test
    void aNonRetryableFailureIsFinalAndUnwrapped() {
        IllegalStateException failure = new IllegalStateException("permanent");
        TaskRecord<String> record = scheduled(c -> { throw failure; });

        run(record);

        assertEquals(TaskStatus.FAILED, record.status());
        assertSame(failure, causeOf(record));
        assertEquals(0, scheduler.pending());
        assertEquals(0, timer.totalArmed());
    }

    @Test
    void aFatalErrorIsNeverRetriedEvenIfTheClassifierWouldAcceptIt() {
        RetryPolicy everything = RetryPolicy.builder().maxAttempts(5).retryOn(Throwable.class).build();
        AssertionError fatal = new AssertionError("fatal");
        TaskRecord<String> record = scheduled(c -> { throw fatal; });

        ExecutionPermit permit = ledger.tryAcquire(0, new int[]{1});
        AssertionError thrown = assertThrows(AssertionError.class,
                () -> new TaskExecution(record, permit, clock, everything, scheduler).run());

        assertSame(fatal, thrown, "fatal errors are still rethrown");
        assertEquals(TaskStatus.FAILED, record.status());
        assertSame(fatal, causeOf(record));
        assertEquals(0, scheduler.pending());
        assertEquals(0, ledger.activeTotal());
    }

    @Test
    void aCancellationExceptionFromUserCodeIsNeverRetried() {
        RetryPolicy everything = RetryPolicy.builder().maxAttempts(5).retryOn(Throwable.class).build();
        CancellationException cancellation = new CancellationException("user cancelled");
        TaskRecord<String> record = scheduled(c -> { throw cancellation; });

        run(record, everything, scheduler);

        assertEquals(TaskStatus.FAILED, record.status());
        assertEquals(0, scheduler.pending());
    }

    @Test
    void whenTheAttemptLimitIsReachedTheFutureFailsWithTheLastFailureAsCause() {
        IOException last = new IOException("third failure");
        TaskRecord<String> record = scheduled(c -> { throw last; });

        assertTrue(record.tryTransition(TaskStatus.RUNNING));
        assertTrue(record.beginRetryWait(new IOException("first")));
        nextAttempt(record);
        assertTrue(record.tryTransition(TaskStatus.RUNNING));
        assertTrue(record.beginRetryWait(new IOException("second")));
        nextAttempt(record);
        assertEquals(3, record.attempt());

        run(record);

        assertEquals(TaskStatus.FAILED, record.status());
        RetryFailedException failure = assertInstanceOf(RetryFailedException.class, causeOf(record));
        assertEquals(RetryFailedException.Reason.ATTEMPTS_EXHAUSTED, failure.reason());
        assertEquals(3, failure.attempts());
        assertSame(last, failure.getCause());
        assertEquals(0, failure.getSuppressed().length);
        assertEquals(0, scheduler.pending());
        assertNull(record.lastFailure());
    }

    @Test
    void aFailureAfterTheDeadlineHasPassedIsNotRetriedAndIsNotWrapped() {
        IOException failure = new IOException("too late");
        TaskRecord<String> record = scheduled(c -> {
            clock.advance(500 * MS);
            throw failure;
        }, true, 100 * MS);

        run(record);

        assertEquals(TaskStatus.FAILED, record.status());
        assertSame(failure, causeOf(record));
        assertEquals(0, scheduler.pending());
    }

    @Test
    void aFailureJustBeforeTheDeadlineIsStillRetried() {
        TaskRecord<String> record = scheduled(c -> {
            clock.advance(99 * MS);
            throw new IOException();
        }, true, 100 * MS);

        run(record);

        assertEquals(TaskStatus.RETRY_WAIT, record.status());
        assertEquals(1, timer.last().delayNanos / MS);
    }

    @Test
    void fullRetryCapacityFailsTheTaskImmediatelyWithAnExplicitReason() {
        RetryScheduler tiny = newScheduler(1);
        IOException failure = new IOException("transient");

        TaskRecord<String> first = scheduled(c -> { throw new IOException("first"); });
        run(first, policy, tiny);
        assertEquals(TaskStatus.RETRY_WAIT, first.status());

        TaskRecord<String> second = TestRecords.queued(2, "q", c -> { throw failure; }, 0, false, 0);
        assertTrue(second.tryTransition(TaskStatus.SCHEDULED));
        run(second, policy, tiny);

        assertEquals(TaskStatus.FAILED, second.status());
        RetryFailedException e = assertInstanceOf(RetryFailedException.class, causeOf(second));
        assertEquals(RetryFailedException.Reason.RETRY_CAPACITY_FULL, e.reason());
        assertSame(failure, e.getCause());
        assertEquals(1, tiny.pending());
    }

    @Test
    void aClosedSchedulerCancelsTheTaskLikeAnyOtherWaitingTaskAtShutdown() {
        scheduler.close();
        TaskRecord<String> record = scheduled(c -> { throw new IOException(); });

        run(record);

        assertEquals(TaskStatus.CANCELLED, record.status());
        assertInstanceOf(CancellationException.class, causeOf(record));
    }

    @Test
    void anUnexpectedSchedulingFailureFailsTheTaskInsteadOfStrandingIt() {
        timer.failWith(new IllegalStateException("timer bug"));
        IOException failure = new IOException("transient");
        TaskRecord<String> record = scheduled(c -> { throw failure; });

        run(record);

        assertEquals(TaskStatus.FAILED, record.status());
        RetryFailedException e = assertInstanceOf(RetryFailedException.class, causeOf(record));
        assertEquals(RetryFailedException.Reason.SCHEDULING_FAILED, e.reason());
        assertSame(failure, e.getCause());
        assertEquals(0, scheduler.pending());
    }

    // ---- cancellation -----------------------------------------------------------

    @Test
    void aTaskCancelledWhileRunningIsNotParkedWhenItThenFails() {
        AtomicReference<TaskRecord<String>> self = new AtomicReference<>();
        TaskRecord<String> record = scheduled(c -> {
            self.get().cancel();
            throw new IOException("failed after cancel");
        });
        self.set(record);

        run(record);

        assertEquals(TaskStatus.CANCELLED, record.status());
        assertEquals(0, scheduler.pending());
        assertEquals(0, timer.totalArmed());
    }

    // ---- attempt tracking -----------------------------------------------------------

    @Test
    void theContextReportsTheRealAttemptNumberOnEveryAttempt() {
        List<Integer> seen = new ArrayList<>();
        AtomicInteger calls = new AtomicInteger();

        TaskRecord<String> record = scheduled(c -> {
            seen.add(c.attempt());
            if (calls.incrementAndGet() < 3) {
                throw new IOException("transient");
            }
            return "done";
        });

        run(record);
        nextAttempt(record);
        run(record);
        nextAttempt(record);
        run(record);

        assertEquals(List.of(1, 2, 3), seen);
        assertEquals(TaskStatus.COMPLETED, record.status());
        assertEquals("done", record.future().join());
        assertNull(record.lastFailure());
        assertEquals(0, scheduler.pending());
    }

    @Test
    void theSameFutureSurvivesEveryAttempt() {
        AtomicInteger calls = new AtomicInteger();
        TaskRecord<String> record = scheduled(c -> {
            if (calls.incrementAndGet() < 2) {
                throw new IOException("transient");
            }
            return "ok";
        });

        run(record);
        assertFalse(record.future().isDone());

        nextAttempt(record);
        run(record);

        assertEquals("ok", record.future().join());
    }

    @Test
    void aThrowingJitterSourceFailsTheTaskInsteadOfStrandingItInRunning() {
        IOException workloadFailure = new IOException("workload failed");
        IllegalStateException policyFailure = new IllegalStateException("jitter source failed");

        RetryPolicy badJitter = RetryPolicy.builder()
                .maxAttempts(3)
                .retryOn(IOException.class)
                .initialBackoff(Duration.ofMillis(10))
                .jitter(0.5)
                .jitterSource(() -> {
                    throw policyFailure;
                })
                .build();

        TaskRecord<String> record = scheduled(c -> { throw workloadFailure; });

        run(record, badJitter, scheduler);

        assertEquals(TaskStatus.FAILED, record.status());
        assertSame(workloadFailure, causeOf(record));
        assertArrayEquals(new Throwable[]{policyFailure}, workloadFailure.getSuppressed());
        assertEquals(0, scheduler.pending());
        assertEquals(0, ledger.activeTotal());
    }

    @Test
    void aClassifierErrorFailsTheTaskBeforeTheFatalErrorIsRethrown() {
        IOException workloadFailure = new IOException("workload failed");
        AssertionError policyFailure = new AssertionError("classifier failed");

        RetryPolicy badClassifier = RetryPolicy.builder()
                .maxAttempts(3)
                .retryIf(failure -> {
                    throw policyFailure;
                })
                .build();

        TaskRecord<String> record = scheduled(c -> { throw workloadFailure; });

        AssertionError thrown = assertThrows(
                AssertionError.class,
                () -> run(record, badClassifier, scheduler)
        );

        assertSame(policyFailure, thrown);
        assertEquals(TaskStatus.FAILED, record.status());
        assertSame(workloadFailure, causeOf(record));
        assertArrayEquals(new Throwable[]{policyFailure}, workloadFailure.getSuppressed());
        assertEquals(0, scheduler.pending());
        assertEquals(0, ledger.activeTotal());
    }

    @Test
    void aJitterSourceErrorFailsTheTaskBeforeTheFatalErrorIsRethrown() {
        IOException workloadFailure = new IOException("workload failed");
        AssertionError policyFailure = new AssertionError("jitter source failed");

        RetryPolicy badJitter = RetryPolicy.builder()
                .maxAttempts(3)
                .retryOn(IOException.class)
                .initialBackoff(Duration.ofMillis(10))
                .jitter(0.5)
                .jitterSource(() -> {
                    throw policyFailure;
                })
                .build();

        TaskRecord<String> record = scheduled(c -> { throw workloadFailure; });

        AssertionError thrown = assertThrows(
                AssertionError.class,
                () -> run(record, badJitter, scheduler)
        );

        assertSame(policyFailure, thrown);
        assertEquals(TaskStatus.FAILED, record.status());
        assertSame(workloadFailure, causeOf(record));
        assertArrayEquals(new Throwable[]{policyFailure}, workloadFailure.getSuppressed());
        assertEquals(0, scheduler.pending());
        assertEquals(0, ledger.activeTotal());
    }
}
