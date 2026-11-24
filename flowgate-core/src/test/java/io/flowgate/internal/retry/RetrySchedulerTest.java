package io.flowgate.internal.retry;

import io.flowgate.api.TaskStatus;
import io.flowgate.exception.RetryFailedException;
import io.flowgate.internal.admission.ReadmissionResult;
import io.flowgate.internal.retry.RetryScheduler.ScheduleResult;
import io.flowgate.internal.task.TaskRecord;
import io.flowgate.testsupport.Concurrently;
import io.flowgate.testsupport.FakeRetryTimer;
import io.flowgate.testsupport.ManualTimeSource;
import io.flowgate.testsupport.TestRecords;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Deterministic tests: the clock and the timer are both controlled by the test. */
class RetrySchedulerTest {

    private static final long MS = 1_000_000L;

    private final ManualTimeSource clock = new ManualTimeSource(1_000L);
    private final FakeRetryTimer timer = new FakeRetryTimer();
    private final List<TaskRecord<?>> readmitted = Collections.synchronizedList(new ArrayList<>());

    private volatile ReadmissionResult nextResult = ReadmissionResult.ADMITTED;

    private RetryScheduler scheduler(int maxPending) {
        return new RetryScheduler(clock, maxPending, record -> {
            readmitted.add(record);
            return nextResult;
        }, timer);
    }

    private static Throwable causeOf(TaskRecord<?> record) {
        CompletionException e = assertThrows(CompletionException.class, () -> record.future().join());
        return e.getCause();
    }

    // ---- timing -----------------------------------------------------------

    @Test
    void nothingHappensBeforeTheBackoffElapses() {
        RetryScheduler scheduler = scheduler(10);
        TaskRecord<String> record = TestRecords.retryWaiting(1);

        assertEquals(ScheduleResult.SCHEDULED, scheduler.schedule(record, 100 * MS));
        assertEquals(1, scheduler.pending());

        clock.advance(100 * MS - 1);

        assertEquals(0, scheduler.runDue());
        assertTrue(readmitted.isEmpty());
        assertEquals(1, scheduler.pending());
    }

    @Test
    void readmitsExactlyOnceAtTheDueInstant() {
        RetryScheduler scheduler = scheduler(10);
        TaskRecord<String> record = TestRecords.retryWaiting(1);
        scheduler.schedule(record, 100 * MS);

        clock.advance(100 * MS);

        assertEquals(1, scheduler.runDue());
        assertEquals(List.of(record), readmitted);
        assertEquals(0, scheduler.pending());

        assertEquals(0, scheduler.runDue(), "a processed entry is never processed again");
        assertEquals(1, readmitted.size());
    }

    @Test
    void entriesAreProcessedInDueOrderThenInsertionOrder() {
        RetryScheduler scheduler = scheduler(10);
        TaskRecord<String> late = TestRecords.retryWaiting(1);
        TaskRecord<String> early = TestRecords.retryWaiting(2);
        TaskRecord<String> tieA = TestRecords.retryWaiting(3);
        TaskRecord<String> tieB = TestRecords.retryWaiting(4);

        scheduler.schedule(late, 300 * MS);
        scheduler.schedule(early, 100 * MS);
        scheduler.schedule(tieA, 200 * MS);
        scheduler.schedule(tieB, 200 * MS);

        clock.advance(400 * MS);
        assertEquals(4, scheduler.runDue());

        assertEquals(List.of(early, tieA, tieB, late), readmitted);
    }

    @Test
    void zeroDelayIsDueImmediately() {
        RetryScheduler scheduler = scheduler(10);
        scheduler.schedule(TestRecords.retryWaiting(1), 0);

        assertEquals(1, scheduler.runDue());
    }

    @Test
    void enormousDelaysAreClampedAndNeverOverflowTheDueTime() {
        ManualTimeSource nearWrap = new ManualTimeSource(Long.MAX_VALUE - 10);
        RetryScheduler scheduler = new RetryScheduler(nearWrap, 10, r -> ReadmissionResult.ADMITTED, timer);

        assertEquals(ScheduleResult.SCHEDULED,
                scheduler.schedule(TestRecords.retryWaiting(1), Long.MAX_VALUE));

        assertTrue(timer.last().delayNanos > 0);
        assertTrue(timer.last().delayNanos <= Long.MAX_VALUE / 4);
        assertEquals(0, scheduler.runDue(), "a clamped far-future entry is not due yet");
    }

    @Test
    void dueTimeIsCorrectAcrossNanoTimeWraparound() {
        ManualTimeSource nearWrap = new ManualTimeSource(Long.MAX_VALUE - 50);
        RetryScheduler scheduler = new RetryScheduler(nearWrap, 10, r -> {
            readmitted.add(r);
            return ReadmissionResult.ADMITTED;
        }, timer);

        scheduler.schedule(TestRecords.retryWaiting(1), 100); // due time wraps negative

        nearWrap.advance(99);
        assertEquals(0, scheduler.runDue());

        nearWrap.advance(1);
        assertEquals(1, scheduler.runDue());
    }

    // ---- the single timer ---------------------------------------------------

    @Test
    void anEarlierEntryReplacesTheOutstandingTimerSoAtMostOneIsEverArmed() {
        RetryScheduler scheduler = scheduler(10);

        scheduler.schedule(TestRecords.retryWaiting(1), 500 * MS);
        assertEquals(1, timer.outstanding());
        assertEquals(500 * MS, timer.last().delayNanos);

        scheduler.schedule(TestRecords.retryWaiting(2), 100 * MS);
        assertEquals(1, timer.outstanding());
        assertEquals(100 * MS, timer.last().delayNanos);
        assertTrue(timer.all().get(0).cancelled());

        scheduler.schedule(TestRecords.retryWaiting(3), 900 * MS);
        assertEquals(1, timer.outstanding(), "a later entry needs no new timer");
        assertEquals(2, timer.totalArmed());
    }

    @Test
    void afterProcessingTheTimerIsRearmedForTheNextEntryOnly() {
        RetryScheduler scheduler = scheduler(10);
        scheduler.schedule(TestRecords.retryWaiting(1), 100 * MS);
        scheduler.schedule(TestRecords.retryWaiting(2), 300 * MS);

        clock.advance(100 * MS);
        timer.all().get(0).handle.complete(null); // the armed timer fires, as a real one would
        scheduler.runDue();

        assertEquals(1, timer.outstanding());
        assertEquals(200 * MS, timer.last().delayNanos);
    }

    @Test
    void noTimerRemainsOnceThereIsNothingToWaitFor() {
        RetryScheduler scheduler = scheduler(10);
        scheduler.schedule(TestRecords.retryWaiting(1), 100 * MS);

        clock.advance(100 * MS);
        scheduler.runDue();
        timer.all().forEach(a -> a.handle.complete(null));

        assertEquals(0, timer.outstanding());
    }

    // ---- bounds --------------------------------------------------------------

    @Test
    void capacityIsAHardBound() {
        RetryScheduler scheduler = scheduler(2);

        assertEquals(ScheduleResult.SCHEDULED, scheduler.schedule(TestRecords.retryWaiting(1), MS));
        assertEquals(ScheduleResult.SCHEDULED, scheduler.schedule(TestRecords.retryWaiting(2), MS));
        assertEquals(ScheduleResult.CAPACITY_FULL, scheduler.schedule(TestRecords.retryWaiting(3), MS));

        assertEquals(2, scheduler.pending());
        assertEquals(2, scheduler.capacity());
    }

    @Test
    void cancelledEntriesAreNotCountedAgainstTheBoundWhenRoomIsNeeded() {
        RetryScheduler scheduler = scheduler(2);
        TaskRecord<String> a = TestRecords.retryWaiting(1);
        TaskRecord<String> b = TestRecords.retryWaiting(2);
        scheduler.schedule(a, 10_000 * MS);
        scheduler.schedule(b, 10_000 * MS);

        assertTrue(a.cancel());

        assertEquals(ScheduleResult.SCHEDULED, scheduler.schedule(TestRecords.retryWaiting(3), MS));
        assertEquals(2, scheduler.pending());
    }

    @Test
    void purgeDeadRemovesCancelledAndExpiredEntriesButKeepsLiveOnes() {
        RetryScheduler scheduler = scheduler(10);
        TaskRecord<String> live = TestRecords.retryWaiting(1);
        TaskRecord<String> cancelled = TestRecords.retryWaiting(2);
        TaskRecord<String> expired = TestRecords.retryWaiting(3);
        scheduler.schedule(live, 10_000 * MS);
        scheduler.schedule(cancelled, 10_000 * MS);
        scheduler.schedule(expired, 10_000 * MS);

        cancelled.cancel();
        expired.expireDuringRetry();

        assertEquals(2, scheduler.purgeDead());
        assertEquals(1, scheduler.pending());
        assertEquals(0, scheduler.purgeDead());
    }

    // ---- cancellation ----------------------------------------------------------

    @Test
    void aTaskCancelledWhileWaitingIsNeverReadmitted() {
        RetryScheduler scheduler = scheduler(10);
        TaskRecord<String> record = TestRecords.retryWaiting(1);
        scheduler.schedule(record, 100 * MS);

        assertTrue(record.cancel());
        clock.advance(100 * MS);

        assertEquals(1, scheduler.runDue(), "the entry is consumed");
        assertTrue(readmitted.isEmpty(), "but a cancelled task must not be re-admitted");
        assertEquals(TaskStatus.CANCELLED, record.status());
    }

    // ---- deadline interaction -----------------------------------------------------

    @Test
    void theDeadlineWakesTheEntryEarlyWhenItComesBeforeTheBackoffEnds() {
        RetryScheduler scheduler = scheduler(10);
        long deadline = clock.nanoTime() + 150 * MS;
        TaskRecord<String> record = TestRecords.retryWaiting(1, "q", true, deadline);

        scheduler.schedule(record, 10_000 * MS);

        assertEquals(150 * MS, timer.last().delayNanos, "woken at the deadline, not after the backoff");
    }

    @Test
    void aTaskWhoseDeadlinePassesDuringBackoffExpiresWithTheLastFailureAsCause() {
        nextResult = ReadmissionResult.DEADLINE_EXPIRED;
        RetryScheduler scheduler = scheduler(10);
        long deadline = clock.nanoTime() + 150 * MS;
        TaskRecord<String> record = TestRecords.retryWaiting(1, "q", true, deadline);
        Throwable last = record.lastFailure();

        scheduler.schedule(record, 10_000 * MS);
        clock.advance(150 * MS);
        scheduler.runDue();

        assertEquals(TaskStatus.EXPIRED, record.status());
        Throwable cause = causeOf(record);
        assertInstanceOf(TimeoutException.class, cause);
        assertSame(last, cause.getCause());
    }

    @Test
    void anAlreadyElapsedDeadlineIsDueImmediately() {
        RetryScheduler scheduler = scheduler(10);
        TaskRecord<String> record = TestRecords.retryWaiting(1, "q", true, clock.nanoTime() - 5);

        scheduler.schedule(record, 10_000 * MS);

        assertEquals(0, timer.last().delayNanos);
        assertEquals(1, scheduler.runDue());
    }

    // ---- readmission outcomes -------------------------------------------------------

    @Test
    void refusedReadmissionFailsTheTaskWithTheLastFailureAndNoBacklog() {
        for (ReadmissionResult refusal : List.of(ReadmissionResult.QUEUE_FULL, ReadmissionResult.GLOBAL_LIMIT)) {
            nextResult = refusal;
            RetryScheduler scheduler = scheduler(10);
            TaskRecord<String> record = TestRecords.retryWaiting(1);
            Throwable last = record.lastFailure();

            scheduler.schedule(record, MS);
            clock.advance(MS);
            scheduler.runDue();

            assertEquals(TaskStatus.FAILED, record.status());
            RetryFailedException failure = assertInstanceOf(RetryFailedException.class, causeOf(record));
            assertEquals(RetryFailedException.Reason.READMISSION_REJECTED, failure.reason());
            assertSame(last, failure.getCause());
            assertEquals(0, scheduler.pending(), "a refused retry is not kept");
            assertEquals(1, readmitted.size(), "exactly one admission attempt");

            readmitted.clear();
        }
    }

    @Test
    void shutdownOutcomeCancelsTheTask() {
        nextResult = ReadmissionResult.SHUTDOWN;
        RetryScheduler scheduler = scheduler(10);
        TaskRecord<String> record = TestRecords.retryWaiting(1);

        scheduler.schedule(record, MS);
        clock.advance(MS);
        scheduler.runDue();

        assertEquals(TaskStatus.CANCELLED, record.status());
        assertInstanceOf(CancellationException.class, causeOf(record));
    }

    @Test
    void admittedAndNotWaitingOutcomesLeaveTheTaskAlone() {
        for (ReadmissionResult benign : List.of(ReadmissionResult.ADMITTED, ReadmissionResult.NOT_WAITING)) {
            nextResult = benign;
            RetryScheduler scheduler = scheduler(10);
            TaskRecord<String> record = TestRecords.retryWaiting(1);

            scheduler.schedule(record, MS);
            clock.advance(MS);
            scheduler.runDue();

            assertEquals(TaskStatus.RETRY_WAIT, record.status());
            assertFalse(record.future().isDone());
        }
    }

    @Test
    void aReadmitterThatThrowsFailsThatTaskAndDoesNotStopTheOthers() {
        List<TaskRecord<?>> seen = new ArrayList<>();
        RetryScheduler scheduler = new RetryScheduler(clock, 10, record -> {
            seen.add(record);
            if (record.id() == 1) {
                throw new IllegalStateException("readmitter bug");
            }
            return ReadmissionResult.ADMITTED;
        }, timer);

        TaskRecord<String> broken = TestRecords.retryWaiting(1);
        TaskRecord<String> healthy = TestRecords.retryWaiting(2);
        scheduler.schedule(broken, MS);
        scheduler.schedule(healthy, 2 * MS);

        clock.advance(2 * MS);
        assertEquals(2, scheduler.runDue());

        assertEquals(2, seen.size());
        assertEquals(TaskStatus.FAILED, broken.status());
        RetryFailedException failure = assertInstanceOf(RetryFailedException.class, causeOf(broken));
        assertEquals(RetryFailedException.Reason.SCHEDULING_FAILED, failure.reason());
        assertEquals(TaskStatus.RETRY_WAIT, healthy.status(), "healthy task was handled normally");
    }

    @Test
    void aFatalErrorStillFinalisesTheTaskAndThenPropagates() {
        RetryScheduler scheduler = new RetryScheduler(clock, 10, record -> {
            throw new AssertionError("fatal");
        }, timer);

        TaskRecord<String> record = TestRecords.retryWaiting(1);
        scheduler.schedule(record, MS);
        clock.advance(MS);

        assertThrows(AssertionError.class, scheduler::runDue);
        assertEquals(TaskStatus.FAILED, record.status(), "no task may be stranded in RETRY_WAIT");
    }

    // ---- shutdown --------------------------------------------------------------------

    @Test
    void closeCancelsEveryParkedTaskAndStopsTheTimer() {
        RetryScheduler scheduler = scheduler(10);
        List<TaskRecord<String>> records = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            TaskRecord<String> record = TestRecords.retryWaiting(i);
            records.add(record);
            scheduler.schedule(record, 10_000 * MS);
        }

        scheduler.close();

        for (TaskRecord<String> record : records) {
            assertEquals(TaskStatus.CANCELLED, record.status());
        }

        assertEquals(0, scheduler.pending());
        assertTrue(scheduler.isClosed());
        assertEquals(0, timer.outstanding());
    }

    @Test
    void afterCloseNothingIsAcceptedOrProcessed() {
        RetryScheduler scheduler = scheduler(10);
        scheduler.schedule(TestRecords.retryWaiting(1), MS);
        scheduler.close();

        TaskRecord<String> late = TestRecords.retryWaiting(2);
        assertEquals(ScheduleResult.CLOSED, scheduler.schedule(late, MS));

        clock.advance(MS);
        assertEquals(0, scheduler.runDue());
        assertTrue(readmitted.isEmpty(), "no retry callback may execute after shutdown");
        assertEquals(TaskStatus.RETRY_WAIT, late.status(), "the caller handles a refused task");
    }

    @Test
    void closeIsIdempotent() {
        RetryScheduler scheduler = scheduler(10);
        TaskRecord<String> record = TestRecords.retryWaiting(1);
        scheduler.schedule(record, MS);

        scheduler.close();
        scheduler.close();

        assertEquals(TaskStatus.CANCELLED, record.status());
    }

    @Test
    void aTimerThatIsGoneMeansClosedAndLeavesNoEntryBehind() {
        RetryScheduler scheduler = scheduler(10);
        timer.reject();

        assertEquals(ScheduleResult.CLOSED, scheduler.schedule(TestRecords.retryWaiting(1), MS));
        assertEquals(0, scheduler.pending());
    }

    @Test
    void anUnexpectedTimerFailureLeavesNoEntryBehindAndPropagates() {
        RetryScheduler scheduler = scheduler(10);
        timer.failWith(new IllegalStateException("timer bug"));

        assertThrows(IllegalStateException.class,
                () -> scheduler.schedule(TestRecords.retryWaiting(1), MS));
        assertEquals(0, scheduler.pending());
    }

    // ---- argument validation -------------------------------------------------------------

    @Test
    void rejectsInvalidArguments() {
        assertThrows(IllegalArgumentException.class,
                () -> new RetryScheduler(clock, 0, r -> ReadmissionResult.ADMITTED, timer));
        assertThrows(NullPointerException.class,
                () -> new RetryScheduler(null, 1, r -> ReadmissionResult.ADMITTED, timer));
        assertThrows(NullPointerException.class, () -> new RetryScheduler(clock, 1, null, timer));
        assertThrows(NullPointerException.class,
                () -> new RetryScheduler(clock, 1, r -> ReadmissionResult.ADMITTED, null));

        RetryScheduler scheduler = scheduler(1);
        assertThrows(IllegalArgumentException.class,
                () -> scheduler.schedule(TestRecords.retryWaiting(1), -1));
        assertThrows(NullPointerException.class, () -> scheduler.schedule(null, 1));
    }

    // ---- concurrency -------------------------------------------------------------------------

    @Test
    void concurrentDriversProcessEveryEntryExactlyOnce() throws Exception {
        final int entries = 2_000;
        Map<Long, AtomicInteger> counts = new ConcurrentHashMap<>();

        RetryScheduler scheduler = new RetryScheduler(clock, entries, record -> {
            counts.computeIfAbsent(record.id(), k -> new AtomicInteger()).incrementAndGet();
            return ReadmissionResult.ADMITTED;
        }, timer);

        for (int i = 0; i < entries; i++) {
            assertEquals(ScheduleResult.SCHEDULED,
                    scheduler.schedule(TestRecords.retryWaiting(i), (i % 50) * MS));
        }

        clock.advance(100 * MS);

        Concurrently.run(8, t -> {
            while (scheduler.pending() > 0) {
                scheduler.runDue();
            }
        });

        assertEquals(entries, counts.size());
        counts.forEach((id, n) -> assertEquals(1, n.get(), "duplicate readmission of " + id));
        assertEquals(0, scheduler.pending());
    }

    @Test
    void schedulingCancellingAndDrivingConcurrentlyNeverDuplicatesOrLoses() throws Exception {
        final int producers = 4;
        final int perProducer = 500;
        final int total = producers * perProducer;

        Map<Long, AtomicInteger> readmits = new ConcurrentHashMap<>();
        List<TaskRecord<String>> all = Collections.synchronizedList(new ArrayList<>());

        RetryScheduler scheduler = new RetryScheduler(clock, total, record -> {
            readmits.computeIfAbsent(record.id(), k -> new AtomicInteger()).incrementAndGet();
            return ReadmissionResult.ADMITTED;
        }, timer);

        Concurrently.run(producers + 2, t -> {
            if (t < producers) {
                for (int n = 0; n < perProducer; n++) {
                    TaskRecord<String> record = TestRecords.retryWaiting((long) t * perProducer + n);
                    all.add(record);
                    scheduler.schedule(record, (n % 10) * MS);
                    clock.advance(MS / 10);
                }
            } else if (t == producers) {
                for (int n = 0; n < 3_000; n++) {
                    TaskRecord<String> record;
                    synchronized (all) {
                        record = all.isEmpty() ? null : all.get(n % all.size());
                    }
                    if (record != null && n % 3 == 0) {
                        record.cancel();
                    }
                }
            } else {
                for (int n = 0; n < 3_000; n++) {
                    scheduler.runDue();
                }
            }
        });

        clock.advance(1_000 * MS);

        while (scheduler.pending() > 0) {
            scheduler.runDue();
        }

        assertEquals(total, all.size());

        for (TaskRecord<String> record : all) {
            int count = readmits.getOrDefault(record.id(), new AtomicInteger()).get();

            assertTrue(count <= 1, "readmitted more than once: " + record.id());

            if (record.status() == TaskStatus.CANCELLED) {
                // cancelled tasks may only have been readmitted if the cancel came after
                assertTrue(count <= 1);
            } else {
                assertEquals(1, count, "task neither cancelled nor readmitted: " + record.id());
            }
        }
    }
}
