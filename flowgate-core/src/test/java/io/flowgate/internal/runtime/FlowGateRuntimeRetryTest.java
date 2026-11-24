
package io.flowgate.internal.runtime;

import io.flowgate.api.Priority;
import io.flowgate.api.Submission;
import io.flowgate.api.TaskHandle;
import io.flowgate.api.TaskStatus;
import io.flowgate.api.WorkloadTask;
import io.flowgate.config.FlowGateConfig;
import io.flowgate.config.QueueConfig;
import io.flowgate.exception.RetryFailedException;
import io.flowgate.internal.time.SystemTimeSource;
import io.flowgate.policy.RetryPolicy;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end retry behaviour on the real runtime, real worker pool and real
 * scheduler thread. Waits are on futures and conditions with generous
 * timeouts, never on fixed sleeps (except where a negative is asserted).
 */
class FlowGateRuntimeRetryTest {

    private static final Duration WAIT = Duration.ofSeconds(15);

    private static RetryPolicy retry(int attempts, Duration initial) {
        return RetryPolicy.builder()
                .maxAttempts(attempts)
                .retryOn(IOException.class)
                .initialBackoff(initial)
                .maxBackoff(initial.multipliedBy(4))
                .multiplier(2.0)
                .build();
    }

    private static QueueConfig queue(String id, int capacity, int max, RetryPolicy retry) {
        return new QueueConfig(id, Priority.NORMAL, 1, capacity, 0, max, null, retry);
    }

    private static FlowGateRuntime runtime(int concurrency, QueueConfig... queues) {
        return new FlowGateRuntime(
                new FlowGateConfig(concurrency, List.of(queues)),
                Duration.ofMillis(2)
        );
    }

    private static FlowGateRuntime runtime(int concurrency, int maxPendingRetries, QueueConfig... queues) {
        return new FlowGateRuntime(
                new FlowGateConfig(concurrency, List.of(queues)),
                Duration.ofMillis(2),
                new SystemTimeSource(),
                maxPendingRetries
        );
    }

    private static <T> TaskHandle<T> submit(FlowGateRuntime runtime, String queue, WorkloadTask<T> task) {
        Submission<T> submission = runtime.submit(queue, task);
        assertTrue(submission.isAccepted(), "unexpected rejection: " + submission);
        return submission.handle();
    }

    private static Throwable failureOf(TaskHandle<?> handle) throws Exception {
        try {
            handle.future().get(WAIT.toMillis(), TimeUnit.MILLISECONDS);
            return null;
        } catch (ExecutionException e) {
            return e.getCause();
        } catch (CancellationException e) {
            return e;
        }
    }

    private static void await(BooleanSupplier condition) {
        long limit = System.nanoTime() + WAIT.toNanos();

        while (!condition.getAsBoolean()) {
            if (System.nanoTime() - limit >= 0) {
                fail("Condition not met within " + WAIT);
            }
            LockSupport.parkNanos(200_000);
        }
    }

    private static void awaitQuiescence(FlowGateRuntime runtime) {
        await(() -> runtime.activeExecutions() == 0
                && runtime.queuedTasks() == 0
                && runtime.pendingRetries() == 0);
    }

    private static long flowgateThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .filter(t -> t.getName().startsWith("flowgate-"))
                .count();
    }

    // ---- basics --------------------------------------------------------------------

    @Test
    void retriesAreOffByDefault() throws Exception {
        QueueConfig plain = new QueueConfig("q", Priority.NORMAL, 1, 10, 0, 1, null);
        AtomicInteger executions = new AtomicInteger();
        IOException failure = new IOException("boom");

        try (FlowGateRuntime runtime = runtime(1, plain)) {
            runtime.start();

            TaskHandle<String> handle = submit(runtime, "q", ctx -> {
                executions.incrementAndGet();
                throw failure;
            });

            assertSame(failure, failureOf(handle));
            assertEquals(TaskStatus.FAILED, handle.status());
            assertEquals(1, executions.get());
            awaitQuiescence(runtime);
        }
    }

    @Test
    void aTaskIsRetriedUntilItSucceedsKeepingOneIdentityAndOneFuture() throws Exception {
        List<Integer> attempts = Collections.synchronizedList(new ArrayList<>());

        try (FlowGateRuntime runtime = runtime(1, queue("q", 10, 1, retry(5, Duration.ofMillis(10))))) {
            runtime.start();

            TaskHandle<String> handle = submit(runtime, "q", ctx -> {
                attempts.add(ctx.attempt());
                if (ctx.attempt() < 3) {
                    throw new IOException("transient #" + ctx.attempt());
                }
                return "done";
            });

            long id = handle.id();
            CompletableFuture<String> future = handle.future();

            assertEquals("done", future.get(WAIT.toMillis(), TimeUnit.MILLISECONDS));
            assertEquals(List.of(1, 2, 3), attempts);
            assertEquals(TaskStatus.COMPLETED, handle.status());
            assertEquals(id, handle.id());
            awaitQuiescence(runtime);
        }
    }

    @Test
    void exhaustedRetriesFailTheFutureWithAnExplicitReasonAndTheLastCause() throws Exception {
        AtomicInteger executions = new AtomicInteger();

        try (FlowGateRuntime runtime = runtime(1, queue("q", 10, 1, retry(3, Duration.ofMillis(5))))) {
            runtime.start();

            TaskHandle<String> handle = submit(runtime, "q", ctx -> {
                throw new IOException("failure on attempt " + executions.incrementAndGet());
            });

            RetryFailedException failure = assertInstanceOf(RetryFailedException.class, failureOf(handle));

            assertEquals(RetryFailedException.Reason.ATTEMPTS_EXHAUSTED, failure.reason());
            assertEquals(3, failure.attempts());
            assertEquals("failure on attempt 3", failure.getCause().getMessage());
            assertEquals(3, executions.get(), "exactly maxAttempts executions");
            assertEquals(TaskStatus.FAILED, handle.status());
            awaitQuiescence(runtime);
        }
    }

    @Test
    void aNonRetryableFailureIsFinalImmediately() throws Exception {
        AtomicInteger executions = new AtomicInteger();
        IllegalStateException failure = new IllegalStateException("permanent");

        try (FlowGateRuntime runtime = runtime(1, queue("q", 10, 1, retry(5, Duration.ofMillis(5))))) {
            runtime.start();

            TaskHandle<String> handle = submit(runtime, "q", ctx -> {
                executions.incrementAndGet();
                throw failure;
            });

            assertSame(failure, failureOf(handle));
            assertEquals(1, executions.get());
            awaitQuiescence(runtime);
        }
    }

    // ---- the worker is never blocked by a backoff --------------------------------------

    @Test
    void aBackoffDoesNotOccupyTheOnlyWorker() throws Exception {
        try (FlowGateRuntime runtime = runtime(1, queue("q", 10, 1, retry(3, Duration.ofSeconds(30))))) {
            runtime.start();

            TaskHandle<String> waiting = submit(runtime, "q", ctx -> { throw new IOException("transient"); });
            await(() -> waiting.status() == TaskStatus.RETRY_WAIT);

            long started = System.nanoTime();
            TaskHandle<String> other = submit(runtime, "q", ctx -> "ran");

            assertEquals("ran", other.future().get(WAIT.toMillis(), TimeUnit.MILLISECONDS));
            assertTrue(System.nanoTime() - started < Duration.ofSeconds(10).toNanos(),
                    "the other task must run while the retry waits out 30 s");
            assertEquals(TaskStatus.RETRY_WAIT, waiting.status());
        }
    }

    // ---- cancellation ---------------------------------------------------------------------

    @Test
    void cancellingDuringBackoffEndsTheTaskAndFreesTheRetrySlot() throws Exception {
        AtomicInteger executions = new AtomicInteger();

        try (FlowGateRuntime runtime = runtime(1, queue("q", 10, 1, retry(3, Duration.ofSeconds(30))))) {
            runtime.start();

            TaskHandle<String> handle = submit(runtime, "q", ctx -> {
                executions.incrementAndGet();
                throw new IOException("transient");
            });

            await(() -> handle.status() == TaskStatus.RETRY_WAIT);
            assertEquals(1, runtime.pendingRetries());

            assertTrue(handle.cancel());

            assertInstanceOf(CancellationException.class, failureOf(handle));
            assertEquals(TaskStatus.CANCELLED, handle.status());

            // The periodic purge releases the dead entry without waiting 30 s.
            await(() -> runtime.pendingRetries() == 0);
            assertEquals(1, executions.get(), "a cancelled task is never resurrected");
            awaitQuiescence(runtime);
        }
    }

    // ---- deadlines -----------------------------------------------------------------------------

    @Test
    void aDeadlineThatPassesDuringBackoffExpiresTheTaskAtTheDeadlineNotAfterTheBackoff() throws Exception {
        try (FlowGateRuntime runtime = runtime(1, queue("q", 10, 1, retry(3, Duration.ofSeconds(30))))) {
            runtime.start();

            long started = System.nanoTime();

            Submission<String> submission = runtime.submit("q",
                    ctx -> { throw new IOException("transient"); }, Duration.ofMillis(150));
            TaskHandle<String> handle = submission.handle();

            Throwable failure = failureOf(handle);
            long elapsed = System.nanoTime() - started;

            assertInstanceOf(TimeoutException.class, failure);
            assertInstanceOf(IOException.class, failure.getCause(), "the last failure is kept as the cause");
            assertEquals(TaskStatus.EXPIRED, handle.status());
            assertTrue(elapsed >= Duration.ofMillis(150).toNanos());
            assertTrue(elapsed < Duration.ofSeconds(10).toNanos(), "must not wait out the 30 s backoff");
            awaitQuiescence(runtime);
        }
    }

    @Test
    void noRetryIsScheduledOnceTheDeadlineHasAlreadyPassed() throws Exception {
        AtomicInteger executions = new AtomicInteger();
        IOException failure = new IOException("late failure");

        try (FlowGateRuntime runtime = runtime(1, queue("q", 10, 1, retry(5, Duration.ofMillis(5))))) {
            runtime.start();

            TaskHandle<String> handle = runtime.<String>submit("q", ctx -> {
                executions.incrementAndGet();
                while (!ctx.isDeadlineExceeded()) {
                    LockSupport.parkNanos(200_000);
                }
                throw failure;
            }, Duration.ofMillis(50)).handle();

            assertSame(failure, failureOf(handle));
            assertEquals(TaskStatus.FAILED, handle.status());
            assertEquals(1, executions.get());
            awaitQuiescence(runtime);
        }
    }

    // ---- refused re-admission and bounds ----------------------------------------------------------

    @Test
    void aFullQueueAtRetryTimeFailsTheTaskWithoutExceedingTheQueueBound() throws Exception {
        CountDownLatch blockerRunning = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        // capacity 1, one worker.
        try (FlowGateRuntime runtime = runtime(1, queue("q", 1, 1, retry(3, Duration.ofMillis(400))))) {
            runtime.start();

            IOException failure = new IOException("transient");
            TaskHandle<String> retried = submit(runtime, "q", ctx -> { throw failure; });
            await(() -> retried.status() == TaskStatus.RETRY_WAIT);

            TaskHandle<String> blocker = submit(runtime, "q", ctx -> {
                blockerRunning.countDown();
                release.await();
                return "blocker";
            });
            assertTrue(blockerRunning.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));

            // The only worker is busy; this fills the single queue slot.
            TaskHandle<String> filler = submit(runtime, "q", ctx -> "filler");

            RetryFailedException e = assertInstanceOf(RetryFailedException.class, failureOf(retried));
            assertEquals(RetryFailedException.Reason.READMISSION_REJECTED, e.reason());
            assertSame(failure, e.getCause());
            assertEquals(TaskStatus.FAILED, retried.status());

            assertEquals(1, runtime.queuedTasks(), "the queue bound was never exceeded");
            assertEquals(0, runtime.pendingRetries());

            release.countDown();
            assertEquals("blocker", blocker.future().get(WAIT.toMillis(), TimeUnit.MILLISECONDS));
            assertEquals("filler", filler.future().get(WAIT.toMillis(), TimeUnit.MILLISECONDS));
            awaitQuiescence(runtime);
        }
    }

    @Test
    void theRetryWaitStoreIsBoundedAndOverflowFailsFast() throws Exception {
        try (FlowGateRuntime runtime = runtime(2, 1, queue("q", 10, 2, retry(3, Duration.ofSeconds(30))))) {
            runtime.start();

            TaskHandle<String> first = submit(runtime, "q", ctx -> { throw new IOException("first"); });
            await(() -> first.status() == TaskStatus.RETRY_WAIT);

            TaskHandle<String> second = submit(runtime, "q", ctx -> { throw new IOException("second"); });

            RetryFailedException e = assertInstanceOf(RetryFailedException.class, failureOf(second));
            assertEquals(RetryFailedException.Reason.RETRY_CAPACITY_FULL, e.reason());
            assertEquals("second", e.getCause().getMessage());

            assertEquals(1, runtime.pendingRetries());
            assertEquals(TaskStatus.RETRY_WAIT, first.status());
        }
    }

    @Test
    void retriesNeverExceedTheConcurrencyBound() throws Exception {
        final int concurrency = 3;
        final int tasks = 60;
        AtomicInteger running = new AtomicInteger();
        AtomicInteger highWater = new AtomicInteger();

        try (FlowGateRuntime runtime = runtime(concurrency,
                queue("q", tasks, concurrency, retry(3, Duration.ofMillis(2))))) {
            runtime.start();

            List<TaskHandle<Integer>> handles = new ArrayList<>();

            for (int i = 0; i < tasks; i++) {
                handles.add(submit(runtime, "q", ctx -> {
                    int now = running.incrementAndGet();
                    highWater.accumulateAndGet(now, Math::max);
                    try {
                        LockSupport.parkNanos(100_000);
                        if (ctx.attempt() < 2) {
                            throw new IOException("first attempt always fails");
                        }
                        return ctx.attempt();
                    } finally {
                        running.decrementAndGet();
                    }
                }));
            }

            for (TaskHandle<Integer> handle : handles) {
                assertEquals(2, handle.future().get(WAIT.toMillis(), TimeUnit.MILLISECONDS));
            }

            assertTrue(highWater.get() <= concurrency, "observed concurrency " + highWater.get());
            awaitQuiescence(runtime);
        }
    }

    // ---- shutdown ------------------------------------------------------------------------------------

    @Test
    void shutdownDuringBackoffCancelsTheWaitingTaskAndReturnsPromptly() throws Exception {
        AtomicInteger executions = new AtomicInteger();

        FlowGateRuntime runtime = runtime(1, queue("q", 10, 1, retry(3, Duration.ofSeconds(30))));
        runtime.start();

        TaskHandle<String> handle = submit(runtime, "q", ctx -> {
            executions.incrementAndGet();
            throw new IOException("transient");
        });
        await(() -> handle.status() == TaskStatus.RETRY_WAIT);

        long started = System.nanoTime();
        runtime.close();

        assertTrue(System.nanoTime() - started < Duration.ofSeconds(10).toNanos(),
                "close must not wait out the backoff");
        assertEquals(TaskStatus.CANCELLED, handle.status());
        assertInstanceOf(CancellationException.class, failureOf(handle));
        assertEquals(0, runtime.pendingRetries());
        assertEquals(0, runtime.queuedTasks());
        assertEquals(0, runtime.activeExecutions());
        assertEquals(1, executions.get(), "no retry callback executes after shutdown");
    }

    @Test
    void aRunningTaskThatFailsDuringShutdownIsCancelledNotRetried() throws Exception {
        CountDownLatch running = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger executions = new AtomicInteger();

        FlowGateRuntime runtime = runtime(1, queue("q", 10, 1, retry(5, Duration.ofMillis(5))));
        runtime.start();

        TaskHandle<String> handle = submit(runtime, "q", ctx -> {
            executions.incrementAndGet();
            running.countDown();
            release.await();
            throw new IOException("fails while the runtime is closing");
        });
        assertTrue(running.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));

        Thread closer = new Thread(runtime::close);
        closer.start();
        await(() -> !runtime.isRunning());
        release.countDown();
        closer.join(WAIT.toMillis());

        assertFalse(closer.isAlive());
        assertEquals(TaskStatus.CANCELLED, handle.status());
        assertEquals(1, executions.get());
        assertEquals(0, runtime.pendingRetries());
        assertEquals(0, runtime.activeExecutions());
    }

    @Test
    void closeIsIdempotentWithPendingRetries() throws Exception {
        FlowGateRuntime runtime = runtime(1, queue("q", 10, 1, retry(3, Duration.ofSeconds(30))));
        runtime.start();

        TaskHandle<String> handle = submit(runtime, "q", ctx -> { throw new IOException(); });
        await(() -> handle.status() == TaskStatus.RETRY_WAIT);

        runtime.close();
        runtime.close();

        assertEquals(TaskStatus.CANCELLED, handle.status());
    }

    @Test
    void closingFromACallbackOnTheSchedulerThreadDoesNotDeadlock() throws Exception {
        CountDownLatch closed = new CountDownLatch(1);

        FlowGateRuntime runtime = runtime(1, queue("q", 10, 1, retry(3, Duration.ofSeconds(30))));
        runtime.start();

        try {
            TaskHandle<String> handle = runtime.<String>submit("q",
                    ctx -> { throw new IOException("transient"); }, Duration.ofMillis(100)).handle();

            // The expiry completes the future on the scheduler thread, which runs this callback.
            handle.future().whenComplete((result, error) -> {
                runtime.close();
                closed.countDown();
            });

            assertTrue(closed.await(WAIT.toMillis(), TimeUnit.MILLISECONDS),
                    "close() from the scheduler thread must not wait for itself");
        } finally {
            runtime.close();
        }
    }

    @Test
    void repeatedStartAndCloseWithRetriesLeavesNothingBehind() throws Exception {
        for (int i = 0; i < 25; i++) {
            FlowGateRuntime runtime = runtime(2, queue("q", 20, 2, retry(4, Duration.ofMillis(1))));
            runtime.start();

            List<TaskHandle<String>> handles = new ArrayList<>();
            for (int n = 0; n < 10; n++) {
                handles.add(submit(runtime, "q", ctx -> { throw new IOException("always"); }));
            }

            runtime.close();

            for (TaskHandle<String> handle : handles) {
                assertNotNull(failureOf(handle), "round " + i + ": every task must end");
                assertTrue(handle.status().isTerminal());
            }

            assertEquals(0, runtime.activeExecutions(), "round " + i);
            assertEquals(0, runtime.queuedTasks(), "round " + i);
            assertEquals(0, runtime.pendingRetries(), "round " + i);
        }
    }

    @Test
    void noSchedulerThreadOrWorkerThreadLeaksAfterClose() throws Exception {
        long before = flowgateThreads();

        for (int i = 0; i < 5; i++) {
            FlowGateRuntime runtime = runtime(2, queue("q", 10, 2, retry(3, Duration.ofSeconds(30))));
            runtime.start();
            TaskHandle<String> handle = submit(runtime, "q", ctx -> { throw new IOException(); });
            await(() -> handle.status() == TaskStatus.RETRY_WAIT);
            runtime.close();
        }

        await(() -> flowgateThreads() <= before);
        assertTrue(flowgateThreads() <= before, "leaked flowgate threads");
    }

    // ---- stress -----------------------------------------------------------------------------------------

    @Test
    void mixedRetryCancellationAndExpiryStressNeverLeaksOrContradicts() throws Exception {
        final int producers = 4;
        final int perProducer = 300;
        final int total = producers * perProducer;
        final int concurrency = 3;

        AtomicIntegerArray executions = new AtomicIntegerArray(total);
        AtomicIntegerArray succeededOnAttempt = new AtomicIntegerArray(total);
        AtomicInteger running = new AtomicInteger();
        AtomicInteger highWater = new AtomicInteger();
        // Indexed by task id, so a handle can never be paired with the wrong id.
        AtomicReferenceArray<TaskHandle<Integer>> handles = new AtomicReferenceArray<>(total);

        RetryPolicy policy = RetryPolicy.builder()
                .maxAttempts(4)
                .retryOn(IOException.class)
                .initialBackoff(Duration.ofMillis(1))
                .maxBackoff(Duration.ofMillis(6))
                .jitter(0.5)
                .build();

        try (FlowGateRuntime runtime = runtime(concurrency,
                queue("q", total, concurrency, policy))) {

            runtime.start();

            Thread[] threads = new Thread[producers];

            for (int p = 0; p < producers; p++) {
                final int producer = p;

                threads[p] = new Thread(() -> {
                    for (int n = 0; n < perProducer; n++) {
                        final int id = producer * perProducer + n;
                        final int failuresBeforeSuccess = id % 5;

                        WorkloadTask<Integer> task = ctx -> {
                            int now = running.incrementAndGet();
                            highWater.accumulateAndGet(now, Math::max);
                            try {
                                executions.incrementAndGet(id);
                                LockSupport.parkNanos(20_000);

                                if (ctx.attempt() <= failuresBeforeSuccess) {
                                    throw new IOException("transient " + id);
                                }

                                succeededOnAttempt.set(id, ctx.attempt());
                                return id;
                            } finally {
                                running.decrementAndGet();
                            }
                        };

                        Duration deadline = id % 11 == 0 ? Duration.ofMillis(3) : null;

                        Submission<Integer> submission = runtime.submit("q", task, deadline);
                        assertTrue(submission.isAccepted(), "unexpected rejection " + submission);

                        TaskHandle<Integer> handle = submission.handle();

                        if (id % 7 == 0) {
                            if (ThreadLocalRandom.current().nextBoolean()) {
                                LockSupport.parkNanos(ThreadLocalRandom.current().nextInt(2_000_000));
                            }
                            handle.cancel();
                        }

                        handles.set(id, handle);
                    }
                });

                threads[p].start();
            }

            for (Thread thread : threads) {
                thread.join(WAIT.toMillis() * 4);
                assertFalse(thread.isAlive(), "producer did not finish");
            }

            int completed = 0;
            int exhausted = 0;
            int cancelled = 0;
            int expired = 0;
            int deadlineFailures = 0;

            for (int id = 0; id < total; id++) {
                TaskHandle<Integer> handle = handles.get(id);
                assertNotNull(handle, "task " + id + " was never submitted");
                Throwable failure = failureOf(handle);
                TaskStatus status = handle.status();
                int runs = executions.get(id);

                assertTrue(status.isTerminal(), "not terminal: " + status);
                assertTrue(runs <= 4, "more executions than maxAttempts for " + id + ": " + runs);

                switch (status) {
                    case COMPLETED -> {
                        completed++;
                        assertNull(failure);
                        assertEquals(id % 5 + 1, runs, "exactly the needed attempts");
                        assertEquals(runs, succeededOnAttempt.get(id));
                    }
                    case FAILED -> {
                        if (failure instanceof RetryFailedException e) {
                            exhausted++;
                            assertEquals(RetryFailedException.Reason.ATTEMPTS_EXHAUSTED, e.reason());
                            assertEquals(4, runs);
                            assertEquals(4, e.attempts());
                        } else {
                            deadlineFailures++;
                            assertInstanceOf(IOException.class, failure);
                            assertEquals(0, id % 11, "raw failure on a task without a deadline: " + id);
                            assertTrue(runs >= 1 && runs <= 4);
                        }
                    }
                    case CANCELLED -> {
                        cancelled++;
                        assertInstanceOf(CancellationException.class, failure);
                    }
                    case EXPIRED -> {
                        expired++;
                        assertInstanceOf(TimeoutException.class, failure);
                    }
                    default -> fail("unexpected terminal status " + status);
                }
            }

            assertEquals(total, completed + exhausted + cancelled + expired + deadlineFailures);
            assertTrue(completed > 0 && exhausted > 0 && cancelled > 0,
                    "scenario should exercise success, exhaustion and cancellation");
            assertTrue(highWater.get() <= concurrency, "observed concurrency " + highWater.get());

            awaitQuiescence(runtime);
        }
    }

    @Test
    void shutdownRacingWithRetryReadmissionNeverStrandsATask() throws Exception {
        for (int round = 0; round < 30; round++) {
            FlowGateRuntime runtime = runtime(2, queue("q", 200, 2, retry(6, Duration.ofMillis(1))));
            runtime.start();

            List<TaskHandle<String>> handles = new ArrayList<>();
            for (int n = 0; n < 100; n++) {
                handles.add(submit(runtime, "q", ctx -> { throw new IOException("always"); }));
            }

            long delayMicros = ThreadLocalRandom.current().nextInt(5_000);
            Thread closer = new Thread(() -> {
                LockSupport.parkNanos(delayMicros * 1_000);
                runtime.close();
            });
            closer.start();
            closer.join(WAIT.toMillis() * 2);
            assertFalse(closer.isAlive(), "round " + round + ": close hung");

            for (TaskHandle<String> handle : handles) {
                assertNotNull(failureOf(handle), "round " + round + ": a task never completed");
                assertTrue(handle.status().isTerminal(), "round " + round + ": " + handle.status());
            }

            assertEquals(0, runtime.queuedTasks(), "round " + round + ": queue slot leaked");
            assertEquals(0, runtime.activeExecutions(), "round " + round + ": permit leaked");
            assertEquals(0, runtime.pendingRetries(), "round " + round + ": retry entry leaked");
        }
    }

    /**
     * Targets the narrow window between a re-admission reserving queue capacity
     * and publishing the task, racing the queue drain performed by close().
     * Without the lifecycle lock in re-admission a task could be published after
     * the drain and be stranded in QUEUED forever. Many rounds, retries with a
     * zero-ish backoff and close() at randomised, very small offsets.
     */
    @Test
    void shutdownDoesNotStrandARetryThatIsBeingReadmittedHeavyStress() throws Exception {
        for (int round = 0; round < 400; round++) {
            FlowGateRuntime runtime = runtime(4, queue("q", 100, 4, RetryPolicy.builder()
                    .maxAttempts(1_000)
                    .retryOn(IOException.class)
                    .initialBackoff(Duration.ofNanos(1))
                    .maxBackoff(Duration.ofNanos(1))
                    .multiplier(1.0)
                    .build()));
            runtime.start();

            List<TaskHandle<String>> handles = new ArrayList<>();
            for (int n = 0; n < 40; n++) {
                handles.add(submit(runtime, "q", ctx -> { throw new IOException("always"); }));
            }

            long spinNanos = ThreadLocalRandom.current().nextInt(300_000);
            long until = System.nanoTime() + spinNanos;
            while (System.nanoTime() - until < 0) {
                Thread.onSpinWait();
            }

            Thread closer = new Thread(runtime::close);
            closer.start();
            closer.join(WAIT.toMillis() * 2);
            assertFalse(closer.isAlive(), "round " + round + ": close hung");

            for (TaskHandle<String> handle : handles) {
                assertTrue(handle.status().isTerminal() || handle.future().isDone() || awaitTerminal(handle),
                        "round " + round + ": task stranded in " + handle.status());
            }

            assertEquals(0, runtime.queuedTasks(), "round " + round + ": queue slot leaked");
            assertEquals(0, runtime.activeExecutions(), "round " + round + ": permit leaked");
            assertEquals(0, runtime.pendingRetries(), "round " + round + ": retry entry leaked");
        }
    }

    private static boolean awaitTerminal(TaskHandle<?> handle) {
        long limit = System.nanoTime() + Duration.ofSeconds(3).toNanos();

        while (!handle.status().isTerminal()) {
            if (System.nanoTime() - limit >= 0) {
                return false;
            }
            LockSupport.parkNanos(100_000);
        }

        return true;
    }

    // ---- the lifecycle-lock protocol (deterministic) -----------------------------------

    /**
     * Re-admission must serialise on lifecycleLock with submit() and close().
     */
    @Test
    void readmissionSerialisesOnTheLifecycleLock() throws Exception {
        try (FlowGateRuntime runtime = runtime(1, queue("q", 5, 1, retry(3, Duration.ofSeconds(30))))) {
            io.flowgate.internal.task.TaskRecord<String> record =
                    io.flowgate.testsupport.TestRecords.retryWaiting(1, "q", false, 0);

            CompletableFuture<io.flowgate.internal.admission.ReadmissionResult> result =
                    new CompletableFuture<>();

            synchronized (runtime.lifecycleLock) {
                Thread thread = new Thread(() -> result.complete(runtime.readmit(record)));
                thread.setDaemon(true);
                thread.start();

                assertThrows(TimeoutException.class, () -> result.get(300, TimeUnit.MILLISECONDS));
                assertEquals(TaskStatus.RETRY_WAIT, record.status());
                assertEquals(0, runtime.queuedTasks());
            }

            assertEquals(io.flowgate.internal.admission.ReadmissionResult.ADMITTED,
                    result.get(WAIT.toMillis(), TimeUnit.MILLISECONDS));
            assertEquals(TaskStatus.QUEUED, record.status());
            assertEquals(1, runtime.queuedTasks());
        }
    }

    @Test
    void readmissionAfterShutdownBeganIsRefusedAndLeavesTheTaskUntouched() {
        FlowGateRuntime runtime = runtime(1, queue("q", 5, 1, retry(3, Duration.ofSeconds(30))));
        runtime.start();
        runtime.close();

        io.flowgate.internal.task.TaskRecord<String> record =
                io.flowgate.testsupport.TestRecords.retryWaiting(1, "q", false, 0);

        assertEquals(io.flowgate.internal.admission.ReadmissionResult.SHUTDOWN, runtime.readmit(record));
        assertEquals(TaskStatus.RETRY_WAIT, record.status());
        assertEquals(0, runtime.queuedTasks());
    }

    @Test
    void schedulerCallbackMustNotWaitForExternalShutdownOwner() throws Exception {
        CountDownLatch callbackEntered = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1);
        CountDownLatch callbackCloseReturned = new CountDownLatch(1);

        FlowGateRuntime runtime = runtime(
                1,
                queue("q", 10, 1, retry(3, Duration.ofSeconds(30)))
        );

        runtime.start();

        try {
            TaskHandle<String> handle = runtime.<String>submit(
                    "q",
                    ctx -> {
                        throw new IOException("transient");
                    },
                    Duration.ofMillis(100)
            ).handle();

            handle.future().whenComplete((result, error) -> {
                callbackEntered.countDown();

                boolean interrupted = false;
                for (;;) {
                    try {
                        releaseCallback.await();
                        break;
                    } catch (InterruptedException ignored) {
                        interrupted = true;
                    }
                }

                try {
                    runtime.close();
                    callbackCloseReturned.countDown();
                } finally {
                    if (interrupted) {
                        Thread.currentThread().interrupt();
                    }
                }
            });

            assertTrue(
                    callbackEntered.await(WAIT.toMillis(), TimeUnit.MILLISECONDS),
                    "Future callback did not execute"
            );

            Thread closer = new Thread(runtime::close, "external-flowgate-closer");
            closer.setDaemon(true);
            closer.start();

            await(() -> {
                for (StackTraceElement frame : closer.getStackTrace()) {
                    if ("stopDeadlineSweeper".equals(frame.getMethodName())) {
                        return true;
                    }
                }
                return false;
            });

            releaseCallback.countDown();

            assertTrue(
                    callbackCloseReturned.await(
                            WAIT.toMillis(),
                            TimeUnit.MILLISECONDS
                    ),
                    "Scheduler callback deadlocked waiting for the shutdown owner"
            );

            closer.join(WAIT.toMillis());

            assertFalse(
                    closer.isAlive(),
                    "External shutdown owner did not finish"
            );

            assertTrue(handle.status().isTerminal());

        } finally {
            releaseCallback.countDown();
            runtime.close();
        }
    }

}
