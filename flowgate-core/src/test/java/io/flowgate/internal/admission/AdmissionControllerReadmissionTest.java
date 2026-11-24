package io.flowgate.internal.admission;

import io.flowgate.api.Priority;
import io.flowgate.api.TaskStatus;
import io.flowgate.config.QueueConfig;
import io.flowgate.internal.queue.GlobalAdmissionGate;
import io.flowgate.internal.queue.QueueRegistry;
import io.flowgate.internal.queue.WorkloadQueue;
import io.flowgate.internal.task.TaskRecord;
import io.flowgate.testsupport.Concurrently;
import io.flowgate.testsupport.ManualTimeSource;
import io.flowgate.testsupport.TestRecords;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Re-admission must use exactly the same capacity protocol as a new submission. */
class AdmissionControllerReadmissionTest {

    private static final long START = 10_000L;

    private final ManualTimeSource clock = new ManualTimeSource(START);

    private GlobalAdmissionGate gate;
    private QueueRegistry registry;
    private AdmissionController admission;
    private WorkloadQueue queue;

    private void setUp(int globalLimit, int queueCapacity) {
        gate = new GlobalAdmissionGate(globalLimit);
        registry = new QueueRegistry(
                List.of(new QueueConfig("q", Priority.NORMAL, 1, queueCapacity, 0, 1, null)),
                gate
        );
        admission = new AdmissionController(registry, gate, clock);
        queue = registry.require("q");
    }

    private TaskRecord<String> waiting(boolean hasDeadline, long deadlineNanos) {
        return TestRecords.retryWaiting(100, "q", hasDeadline, deadlineNanos);
    }

    @Test
    void admittedRetryReusesTheSameRecordAndConsumesCapacityLikeASubmission() {
        setUp(10, 5);
        TaskRecord<String> record = waiting(false, 0);

        assertEquals(0, queue.size());
        assertEquals(0, gate.held());

        assertEquals(ReadmissionResult.ADMITTED, admission.readmit(record));

        assertEquals(TaskStatus.QUEUED, record.status());
        assertEquals(2, record.attempt());
        assertEquals(1, queue.size());
        assertEquals(1, gate.held());
        assertSame(record, queue.peek(), "same record, therefore same id and future");
        assertEquals(START, record.enqueueNanos());
    }

    @Test
    void readmissionPassesThroughTheNormalDequeueReleasePath() {
        setUp(10, 5);
        TaskRecord<String> record = waiting(false, 0);

        admission.readmit(record);
        assertSame(record, queue.poll());

        assertEquals(0, queue.size());
        assertEquals(0, gate.held());
    }

    @Test
    void refusedWhenTheQueueIsFullAndNothingIsReserved() {
        setUp(10, 1);
        assertTrue(admission.submit("q", c -> "x").isAccepted());

        TaskRecord<String> record = waiting(false, 0);

        assertEquals(ReadmissionResult.QUEUE_FULL, admission.readmit(record));

        assertEquals(TaskStatus.RETRY_WAIT, record.status());
        assertEquals(1, record.attempt());
        assertEquals(1, queue.size());
        assertEquals(1, gate.held());
    }

    @Test
    void refusedWhenGlobalAdmissionIsExhaustedAndNothingIsReserved() {
        setUp(1, 5);
        assertTrue(admission.submit("q", c -> "x").isAccepted());

        TaskRecord<String> record = waiting(false, 0);

        assertEquals(ReadmissionResult.GLOBAL_LIMIT, admission.readmit(record));

        assertEquals(TaskStatus.RETRY_WAIT, record.status());
        assertEquals(1, queue.size(), "no local reservation may leak");
        assertEquals(1, gate.held());
    }

    @Test
    void retriesCannotExceedTheQueueBound() {
        setUp(100, 3);

        for (int i = 0; i < 10; i++) {
            admission.readmit(TestRecords.retryWaiting(i, "q", false, 0));
        }

        assertEquals(3, queue.size());
        assertEquals(3, gate.held());
    }

    @Test
    void retriesAndSubmissionsShareTheGlobalBound() {
        setUp(4, 10);

        assertTrue(admission.submit("q", c -> "x").isAccepted());
        assertTrue(admission.submit("q", c -> "x").isAccepted());

        assertEquals(ReadmissionResult.ADMITTED,
                admission.readmit(TestRecords.retryWaiting(1, "q", false, 0)));
        assertEquals(ReadmissionResult.ADMITTED,
                admission.readmit(TestRecords.retryWaiting(2, "q", false, 0)));
        assertEquals(ReadmissionResult.GLOBAL_LIMIT,
                admission.readmit(TestRecords.retryWaiting(3, "q", false, 0)));
        assertTrue(admission.submit("q", c -> "x").isRejected());

        assertEquals(4, gate.held());
    }

    @Test
    void expiredDeadlineIsReportedWithoutReservingAnything() {
        setUp(10, 5);
        TaskRecord<String> record = waiting(true, START + 100);

        clock.advance(100);

        assertEquals(ReadmissionResult.DEADLINE_EXPIRED, admission.readmit(record));

        assertEquals(TaskStatus.RETRY_WAIT, record.status(), "the caller finalises expiry");
        assertEquals(0, queue.size());
        assertEquals(0, gate.held());
    }

    @Test
    void deadlineBoundaryIsExclusiveJustBeforeInclusiveAtTheDeadline() {
        setUp(10, 5);

        TaskRecord<String> justBefore = waiting(true, START + 100);
        clock.advance(99);
        assertEquals(ReadmissionResult.ADMITTED, admission.readmit(justBefore));
        assertEquals(1L, justBefore.remainingTimeNanos(clock.nanoTime()), "deadline is never reset");

        TaskRecord<String> atDeadline = TestRecords.retryWaiting(2, "q", true, START + 100);
        clock.advance(1);
        assertEquals(ReadmissionResult.DEADLINE_EXPIRED, admission.readmit(atDeadline));
    }

    @Test
    void taskNoLongerWaitingIsLeftAlone() {
        setUp(10, 5);
        TaskRecord<String> record = waiting(false, 0);
        assertTrue(record.cancel());

        assertEquals(ReadmissionResult.NOT_WAITING, admission.readmit(record));

        assertEquals(TaskStatus.CANCELLED, record.status());
        assertEquals(0, queue.size());
        assertEquals(0, gate.held());
    }

    @Test
    void runningOrQueuedTasksAreNotReadmitted() {
        setUp(10, 5);

        assertEquals(ReadmissionResult.NOT_WAITING, admission.readmit(
                TestRecords.running(1, "q", c -> "x", 0, false, 0)));
        assertEquals(ReadmissionResult.NOT_WAITING, admission.readmit(
                TestRecords.queued(2, "q", c -> "x", 0, false, 0)));

        assertEquals(0, queue.size());
    }

    @Test
    void refusedAfterShutdownAndNothingIsReserved() {
        setUp(10, 5);
        TaskRecord<String> record = waiting(false, 0);

        admission.shutdown();

        assertEquals(ReadmissionResult.SHUTDOWN, admission.readmit(record));
        assertEquals(TaskStatus.RETRY_WAIT, record.status());
        assertEquals(0, queue.size());
        assertEquals(0, gate.held());
    }

    @Test
    void doubleReadmissionOfOneRecordNeverQueuesItTwice() {
        setUp(10, 5);
        TaskRecord<String> record = waiting(false, 0);

        assertEquals(ReadmissionResult.ADMITTED, admission.readmit(record));
        assertEquals(ReadmissionResult.NOT_WAITING, admission.readmit(record));

        assertEquals(1, queue.size());
        assertEquals(1, gate.held());
        assertEquals(2, record.attempt());
    }

    @Test
    void concurrentReadmissionOfOneRecordAdmitsItExactlyOnce() throws Exception {
        for (int round = 0; round < 200; round++) {
            setUp(10, 5);
            TaskRecord<String> record = waiting(false, 0);
            AtomicInteger admitted = new AtomicInteger();

            Concurrently.run(8, i -> {
                if (admission.readmit(record) == ReadmissionResult.ADMITTED) {
                    admitted.incrementAndGet();
                }
            });

            assertEquals(1, admitted.get());
            assertEquals(1, queue.size());
            assertEquals(1, gate.held(), "no double queue or global reservation");
            assertEquals(2, record.attempt());
        }
    }

    @Test
    void cancellationRacingWithReadmissionNeverLeaksCapacity() throws Exception {
        for (int round = 0; round < 500; round++) {
            setUp(10, 5);
            TaskRecord<String> record = waiting(false, 0);

            Concurrently.run(2, i -> {
                if (i == 0) {
                    admission.readmit(record);
                } else {
                    record.cancel();
                }
            });

            assertEquals(TaskStatus.CANCELLED, record.status());

            // If readmission won it published the (now cancelled) record, which
            // the dispatcher will discard through the normal poll path; if
            // cancellation won, nothing may remain reserved.
            int published = queue.size();
            assertEquals(published, gate.held());
            assertTrue(published == 0 || published == 1);

            while (queue.poll() != null) {
                // normal dequeue releases both capacities
            }

            assertEquals(0, queue.size());
            assertEquals(0, gate.held());
        }
    }

    @Test
    void newSubmissionsAreUnaffectedByTheRetryApi() {
        setUp(10, 5);

        assertTrue(admission.submit("q", c -> "x", Duration.ofSeconds(1)).isAccepted());
        assertEquals(1, queue.size());
        assertEquals(1, ((TaskRecord<?>) queue.peek()).attempt());
    }
}
