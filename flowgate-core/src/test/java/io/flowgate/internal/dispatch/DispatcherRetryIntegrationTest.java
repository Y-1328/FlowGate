package io.flowgate.internal.dispatch;

import io.flowgate.api.Priority;
import io.flowgate.api.TaskHandle;
import io.flowgate.api.TaskStatus;
import io.flowgate.api.WorkloadTask;
import io.flowgate.config.QueueConfig;
import io.flowgate.exception.RetryFailedException;
import io.flowgate.internal.admission.AdmissionController;
import io.flowgate.internal.ledger.ConcurrencyLedger;
import io.flowgate.internal.queue.GlobalAdmissionGate;
import io.flowgate.internal.queue.QueueRegistry;
import io.flowgate.internal.queue.WorkloadQueue;
import io.flowgate.internal.retry.RetryScheduler;
import io.flowgate.internal.task.TaskRecord;
import io.flowgate.policy.PriorityAwareAgingPolicy;
import io.flowgate.policy.RetryPolicy;
import io.flowgate.testsupport.FakeRetryTimer;
import io.flowgate.testsupport.ManualTimeSource;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The whole pipeline, deterministically: real admission, queues, DRR scheduler,
 * ledger, dispatcher, worker execution and retry scheduler, driven by a manual
 * clock and a fake timer. Retries are only ever executed by dispatchOne().
 */
class DispatcherRetryIntegrationTest {

    private static final long MS = 1_000_000L;

    private static final RetryPolicy RETRY_3 = RetryPolicy.builder()
            .maxAttempts(3)
            .retryOn(IOException.class)
            .initialBackoff(Duration.ofMillis(100))
            .maxBackoff(Duration.ofSeconds(10))
            .multiplier(2.0)
            .build();

    private static QueueConfig queue(String id, int capacity, RetryPolicy retry) {
        return new QueueConfig(id, Priority.NORMAL, 1, capacity, 0, 1, null, retry);
    }

    private static final class Fixture {

        final ManualTimeSource clock = new ManualTimeSource(1_000_000_000L);
        final GlobalAdmissionGate gate;
        final QueueRegistry registry;
        final AdmissionController admission;
        final ConcurrencyLedger ledger;
        final DeficitRoundRobinScheduler drr;
        final FakeRetryTimer timer = new FakeRetryTimer();
        final RetryScheduler retries;
        final List<Runnable> workers = new ArrayList<>();
        final Dispatcher dispatcher;

        Fixture(int globalLimit, int maxPending, int agingMaxBoost, QueueConfig... configs) {
            gate = new GlobalAdmissionGate(globalLimit);
            registry = new QueueRegistry(List.of(configs), gate);
            admission = new AdmissionController(registry, gate, clock);
            ledger = new ConcurrencyLedger(1, registry);
            drr = new DeficitRoundRobinScheduler(configs.length);
            retries = new RetryScheduler(clock, maxPending, admission::readmit, timer);

            int[] quantum = new int[configs.length];
            java.util.Arrays.fill(quantum, 1);

            dispatcher = new Dispatcher(
                    registry, ledger, drr, workers::add, clock, quantum,
                    new PriorityAwareAgingPolicy(1_000_000_000L, agingMaxBoost, Map.of()),
                    retries
            );
        }

        Fixture(QueueConfig... configs) {
            this(1_000, 1_000, 0, configs);
        }

        <T> TaskHandle<T> submit(String queueId, WorkloadTask<T> task) {
            var submission = admission.submit(queueId, task);
            assertTrue(submission.isAccepted(), "unexpected rejection: " + submission);
            return submission.handle();
        }

        /** One dispatch decision followed by running the handed-off attempt. */
        void dispatchAndRun() {
            assertTrue(dispatcher.dispatchOne(), "nothing was dispatchable");
            assertEquals(1, workers.size());
            workers.remove(0).run();
        }

        /** Lets time pass and lets the retry scheduler act, as its timer would. */
        int elapse(Duration duration) {
            clock.advance(duration);
            return retries.runDue();
        }

        int queued(String queueId) {
            return registry.require(queueId).size();
        }

        void assertNothingLeaked() {
            assertEquals(0, ledger.activeTotal(), "execution permit leaked");
            assertEquals(0, gate.held(), "global admission capacity leaked");
            assertEquals(0, retries.pending(), "retry entry leaked");

            for (WorkloadQueue q : registry.ordered()) {
                assertEquals(0, q.size(), "queue capacity leaked");
            }
        }
    }

    // ---- the happy path of a retried task ------------------------------------------

    @Test
    void aTaskFailsTwiceThenSucceedsWithOneIdentityOneFutureAndExactBackoff() {
        Fixture f = new Fixture(queue("q", 10, RETRY_3));
        List<Integer> attempts = new ArrayList<>();

        TaskHandle<String> handle = f.submit("q", ctx -> {
            attempts.add(ctx.attempt());
            if (ctx.attempt() < 3) {
                throw new IOException("transient #" + ctx.attempt());
            }
            return "ok";
        });

        long id = handle.id();
        var future = handle.future();

        // Attempt 1 fails.
        f.dispatchAndRun();
        assertEquals(TaskStatus.RETRY_WAIT, handle.status());
        assertEquals(0, f.queued("q"), "waiting for a retry holds no queue capacity");
        assertEquals(1, f.retries.pending());
        assertEquals(0, f.ledger.activeTotal());
        assertFalse(future.isDone());
        assertFalse(f.dispatcher.dispatchOne(), "nothing is dispatchable during backoff");

        // First backoff is 100 ms: not a nanosecond earlier.
        assertEquals(0, f.elapse(Duration.ofNanos(100 * MS - 1)));
        assertEquals(TaskStatus.RETRY_WAIT, handle.status());
        assertEquals(1, f.elapse(Duration.ofNanos(1)));
        assertEquals(TaskStatus.QUEUED, handle.status());
        assertEquals(1, f.queued("q"));
        assertEquals(1, f.gate.held());

        // Attempt 2 fails; second backoff is 200 ms.
        f.dispatchAndRun();
        assertEquals(TaskStatus.RETRY_WAIT, handle.status());
        assertEquals(0, f.elapse(Duration.ofNanos(200 * MS - 1)));
        assertEquals(1, f.elapse(Duration.ofNanos(1)));

        // Attempt 3 succeeds.
        f.dispatchAndRun();

        assertEquals(TaskStatus.COMPLETED, handle.status());
        assertEquals("ok", future.join());
        assertEquals(List.of(1, 2, 3), attempts);
        assertEquals(id, handle.id());
        f.assertNothingLeaked();
    }

    @Test
    void exhaustedRetriesFailTheFutureWithTheLastFailureAndLeakNothing() {
        Fixture f = new Fixture(queue("q", 10, RETRY_3));
        List<IOException> failures = new ArrayList<>();

        TaskHandle<String> handle = f.submit("q", ctx -> {
            IOException e = new IOException("failure " + ctx.attempt());
            failures.add(e);
            throw e;
        });

        for (int attempt = 1; attempt <= 3; attempt++) {
            f.dispatchAndRun();

            if (attempt < 3) {
                assertEquals(TaskStatus.RETRY_WAIT, handle.status());
                f.elapse(Duration.ofSeconds(10));
            }
        }

        assertEquals(TaskStatus.FAILED, handle.status());
        CompletionException e = assertThrows(CompletionException.class, () -> handle.future().join());
        RetryFailedException failure = assertInstanceOf(RetryFailedException.class, e.getCause());

        assertEquals(RetryFailedException.Reason.ATTEMPTS_EXHAUSTED, failure.reason());
        assertEquals(3, failure.attempts());
        assertSame(failures.get(2), failure.getCause());
        assertEquals(3, failures.size(), "executed exactly maxAttempts times");
        assertFalse(f.dispatcher.dispatchOne());
        f.assertNothingLeaked();
    }

    @Test
    void retriesAreOffByDefaultInTheIntegratedPipeline() {
        Fixture f = new Fixture(queue("q", 10, null));
        IOException failure = new IOException("boom");

        TaskHandle<String> handle = f.submit("q", ctx -> { throw failure; });
        f.dispatchAndRun();

        assertEquals(TaskStatus.FAILED, handle.status());
        assertSame(failure, assertThrows(CompletionException.class, () -> handle.future().join()).getCause());
        f.assertNothingLeaked();
    }

    // ---- no privileged path --------------------------------------------------------------

    @Test
    void aRetryJoinsTheBackOfItsQueueAndIsNotPrivileged() {
        Fixture f = new Fixture(queue("q", 10, RETRY_3));
        List<String> order = new ArrayList<>();

        boolean[] failedOnce = {false};
        TaskHandle<String> retried = f.submit("q", ctx -> {
            order.add("R" + ctx.attempt());
            if (!failedOnce[0]) {
                failedOnce[0] = true;
                throw new IOException();
            }
            return "ok";
        });

        f.dispatchAndRun();                         // R attempt 1 fails
        TaskHandle<String> x1 = f.submit("q", ctx -> { order.add("X1"); return "x"; });
        TaskHandle<String> x2 = f.submit("q", ctx -> { order.add("X2"); return "x"; });

        f.elapse(Duration.ofMillis(100));           // R re-enters behind X1, X2

        f.dispatchAndRun();
        f.dispatchAndRun();
        f.dispatchAndRun();

        assertEquals(List.of("R1", "X1", "X2", "R2"), order);
        assertEquals(TaskStatus.COMPLETED, retried.status());
        assertEquals(TaskStatus.COMPLETED, x1.status());
        assertEquals(TaskStatus.COMPLETED, x2.status());
        f.assertNothingLeaked();
    }

    @Test
    void retriesParticipateInNormalWeightedRoundRobinAcrossQueues() {
        Fixture f = new Fixture(queue("a", 10, RETRY_3), queue("b", 10, null));
        List<String> order = new ArrayList<>();

        boolean[] failedOnce = {false};
        f.submit("a", ctx -> {
            order.add("A-retry#" + ctx.attempt());
            if (!failedOnce[0]) {
                failedOnce[0] = true;
                throw new IOException();
            }
            return "ok";
        });

        f.dispatchAndRun();                         // the retried task fails once

        f.submit("a", ctx -> { order.add("A2"); return "x"; });
        f.submit("b", ctx -> { order.add("B1"); return "x"; });
        f.submit("b", ctx -> { order.add("B2"); return "x"; });

        f.elapse(Duration.ofMillis(100));           // a: [A2, retry], b: [B1, B2]

        for (int i = 0; i < 4; i++) {
            f.dispatchAndRun();
        }

        // After queue a's first dispatch the DRR cursor is on b, so the queues
        // strictly alternate b, a, b, a. The retry sits behind A2 in a's FIFO
        // and gets one turn per round, exactly like any other task: no priority
        // lane and no extra turns.
        assertEquals(List.of("A-retry#1", "B1", "A2", "B2", "A-retry#2"), order);
        f.assertNothingLeaked();
    }

    @Test
    void aRetryNeverRunsWithoutAPermit() {
        Fixture f = new Fixture(queue("q", 10, RETRY_3));
        boolean[] failedOnce = {false};

        f.submit("q", ctx -> {
            if (!failedOnce[0]) {
                failedOnce[0] = true;
                throw new IOException();
            }
            return "ok";
        });

        f.dispatchAndRun();
        f.elapse(Duration.ofMillis(100));

        // Take the only execution permit.
        var held = f.ledger.tryAcquire(0, new int[]{1});
        assertNotNull(held);

        assertFalse(f.dispatcher.dispatchOne(), "no permit, so the retry must not start");
        assertEquals(0, f.workers.size());

        held.close();
        f.dispatchAndRun();
        f.assertNothingLeaked();
    }

    @Test
    void aRetryRestartsPriorityAgingFromItsReadmissionNotFromItsFirstSubmission() {
        // With aging enabled, a head that has waited 10 s would earn the maximum boost.
        Fixture f = new Fixture(1_000, 1_000, 3, queue("q", 10, RETRY_3));
        boolean[] failedOnce = {false};

        TaskHandle<String> handle = f.submit("q", ctx -> {
            if (!failedOnce[0]) {
                failedOnce[0] = true;
                throw new IOException();
            }
            return "ok";
        });

        f.dispatchAndRun();
        f.clock.advance(Duration.ofSeconds(10));    // long after the original submission
        f.retries.runDue();                          // re-admitted now

        TaskRecord<?> head = (TaskRecord<?>) f.registry.require("q").peek();
        assertSame(handle, head);
        assertEquals(f.clock.nanoTime(), head.enqueueNanos());

        f.dispatchAndRun();

        // quantum was the base weight (1), not boosted: one dispatch consumed it all.
        assertEquals(0, f.drr.deficit(0));
        assertEquals(TaskStatus.COMPLETED, handle.status());
    }

    // ---- refused re-admission --------------------------------------------------------------

    @Test
    void aFullQueueRefusesTheRetryWhichThenFailsWithoutAnyHiddenBacklog() {
        Fixture f = new Fixture(queue("q", 1, RETRY_3));
        IOException failure = new IOException("transient");

        TaskHandle<String> retried = f.submit("q", ctx -> { throw failure; });
        f.dispatchAndRun();
        assertEquals(TaskStatus.RETRY_WAIT, retried.status());

        TaskHandle<String> blocker = f.submit("q", ctx -> "fills the queue");
        assertEquals(1, f.queued("q"));

        f.elapse(Duration.ofMillis(100));

        assertEquals(TaskStatus.FAILED, retried.status());
        RetryFailedException e = assertInstanceOf(RetryFailedException.class,
                assertThrows(CompletionException.class, () -> retried.future().join()).getCause());
        assertEquals(RetryFailedException.Reason.READMISSION_REJECTED, e.reason());
        assertSame(failure, e.getCause());

        assertEquals(1, f.queued("q"), "the bound is intact");
        assertEquals(1, f.gate.held());
        assertEquals(0, f.retries.pending(), "a refused retry is not retained");
        assertEquals(TaskStatus.QUEUED, blocker.status(), "other work is unaffected");

        f.dispatchAndRun();
        f.assertNothingLeaked();
    }

    @Test
    void exhaustedGlobalAdmissionAlsoRefusesTheRetry() {
        Fixture f = new Fixture(1, 100, 0, queue("q", 10, RETRY_3));

        TaskHandle<String> retried = f.submit("q", ctx -> { throw new IOException(); });
        f.dispatchAndRun();                          // gate now free again

        f.submit("q", ctx -> "takes the only global slot");
        assertEquals(1, f.gate.held());

        f.elapse(Duration.ofMillis(100));

        assertEquals(TaskStatus.FAILED, retried.status());
        RetryFailedException e = assertInstanceOf(RetryFailedException.class,
                assertThrows(CompletionException.class, () -> retried.future().join()).getCause());
        assertEquals(RetryFailedException.Reason.READMISSION_REJECTED, e.reason());
        assertEquals(1, f.gate.held());
    }

    @Test
    void theRetryWaitStoreIsBoundedIndependentlyOfAdmission() {
        Fixture f = new Fixture(1_000, 2, 0, queue("q", 10, RETRY_3));
        List<TaskHandle<String>> handles = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            handles.add(f.submit("q", ctx -> { throw new IOException("transient"); }));
        }

        for (int i = 0; i < 3; i++) {
            f.dispatchAndRun();
        }

        assertEquals(TaskStatus.RETRY_WAIT, handles.get(0).status());
        assertEquals(TaskStatus.RETRY_WAIT, handles.get(1).status());
        assertEquals(TaskStatus.FAILED, handles.get(2).status(), "third had no retry slot");
        assertEquals(2, f.retries.pending());

        RetryFailedException e = assertInstanceOf(RetryFailedException.class,
                assertThrows(CompletionException.class, () -> handles.get(2).future().join()).getCause());
        assertEquals(RetryFailedException.Reason.RETRY_CAPACITY_FULL, e.reason());
    }

    // ---- deadline -------------------------------------------------------------------------------

    @Test
    void theOriginalDeadlineStaysAuthoritativeAcrossAttempts() {
        Fixture f = new Fixture(queue("q", 10, RETRY_3));
        boolean[] failedOnce = {false};

        TaskHandle<String> handle = f.admission.submit("q", ctx -> {
            if (!failedOnce[0]) {
                failedOnce[0] = true;
                throw new IOException();
            }
            return "ok";
        }, Duration.ofSeconds(1)).handle();

        long remainingAtStart = ((TaskRecord<?>) handle).remainingTimeNanos(f.clock.nanoTime());

        f.dispatchAndRun();
        f.elapse(Duration.ofMillis(100));           // re-admitted after the 100 ms backoff

        TaskRecord<?> record = (TaskRecord<?>) handle;
        assertEquals(remainingAtStart - 100 * MS, record.remainingTimeNanos(f.clock.nanoTime()),
                "the deadline must not be reset by the retry");
    }

    @Test
    void aTaskWhoseDeadlinePassesDuringBackoffExpiresAndIsNeverReadmitted() {
        Fixture f = new Fixture(queue("q", 10, RETRY_3));
        IOException failure = new IOException("transient");

        TaskHandle<String> handle = f.admission.<String>submit("q", ctx -> { throw failure; },
                Duration.ofMillis(150)).handle();

        f.dispatchAndRun();                          // fails; backoff 100 ms < deadline 150 ms
        assertEquals(TaskStatus.RETRY_WAIT, handle.status());

        f.elapse(Duration.ofMillis(100));            // readmitted at 100 ms
        assertEquals(TaskStatus.QUEUED, handle.status());
        f.dispatchAndRun();                          // attempt 2 fails at 100 ms: backoff 200 ms > remaining 50 ms

        assertEquals(TaskStatus.RETRY_WAIT, handle.status());
        assertEquals(0, f.elapse(Duration.ofMillis(49)));
        assertEquals(1, f.elapse(Duration.ofMillis(1)), "woken at the deadline, not after the backoff");

        assertEquals(TaskStatus.EXPIRED, handle.status());
        Throwable cause = assertThrows(CompletionException.class, () -> handle.future().join()).getCause();
        assertInstanceOf(TimeoutException.class, cause);
        assertSame(failure.getClass(), cause.getCause().getClass());
        assertEquals(0, f.queued("q"));
        assertFalse(f.dispatcher.dispatchOne(), "an expired task is never dispatched again");
        f.assertNothingLeaked();
    }

    // ---- cancellation ------------------------------------------------------------------------------

    @Test
    void cancellingDuringBackoffStopsTheRetryForGood() {
        Fixture f = new Fixture(queue("q", 10, RETRY_3));
        int[] executions = {0};

        TaskHandle<String> handle = f.submit("q", ctx -> {
            executions[0]++;
            throw new IOException();
        });

        f.dispatchAndRun();
        assertEquals(TaskStatus.RETRY_WAIT, handle.status());

        assertTrue(handle.cancel());
        assertEquals(TaskStatus.CANCELLED, handle.status());
        assertInstanceOf(CancellationException.class,
                assertThrows(CompletionException.class, () -> handle.future().join()).getCause());

        f.elapse(Duration.ofSeconds(60));

        assertEquals(TaskStatus.CANCELLED, handle.status(), "never resurrected by the retry timer");
        assertEquals(0, f.queued("q"));
        assertFalse(f.dispatcher.dispatchOne());
        assertEquals(1, executions[0]);
        f.assertNothingLeaked();
    }

    @Test
    void cancellingAfterReadmissionIsOrdinaryQueuedCancellation() {
        Fixture f = new Fixture(queue("q", 10, RETRY_3));
        TaskHandle<String> handle = f.submit("q", ctx -> { throw new IOException(); });

        f.dispatchAndRun();
        f.elapse(Duration.ofMillis(100));
        assertEquals(TaskStatus.QUEUED, handle.status());

        assertTrue(handle.cancel());

        // The dispatcher discards the cancelled task through the normal path.
        assertFalse(f.dispatcher.dispatchOne());
        f.assertNothingLeaked();
    }

    @Test
    void closingTheRetrySchedulerCancelsWaitingTasksAndNothingRunsAfterwards() {
        Fixture f = new Fixture(queue("q", 10, RETRY_3));
        int[] executions = {0};

        TaskHandle<String> handle = f.submit("q", ctx -> {
            executions[0]++;
            throw new IOException();
        });

        f.dispatchAndRun();
        f.retries.close();

        assertEquals(TaskStatus.CANCELLED, handle.status());

        f.elapse(Duration.ofSeconds(60));

        assertFalse(f.dispatcher.dispatchOne());
        assertEquals(1, executions[0]);
        f.assertNothingLeaked();
    }
}
