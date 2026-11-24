
package io.flowgate.internal.runtime;

import io.flowgate.api.Priority;
import io.flowgate.api.RejectionReason;
import io.flowgate.api.Submission;
import io.flowgate.api.TaskStatus;
import io.flowgate.config.FlowGateConfig;
import io.flowgate.config.QueueConfig;

import java.util.concurrent.*;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

class FlowGateRuntimeTest {

    private static final Duration WAIT = Duration.ofSeconds(3);

    private FlowGateRuntime createRuntime() {
        QueueConfig queue = new QueueConfig(
                "default",
                Priority.NORMAL,
                1,
                8,
                1,
                1,
                null
        );

        FlowGateConfig config = new FlowGateConfig(
                1,
                List.of(queue)
        );

        return new FlowGateRuntime(
                config,
                Duration.ofMillis(20)
        );
    }

    @Test
    void shouldExecuteTaskSubmittedBeforeStartup() throws Exception {
        FlowGateRuntime runtime = createRuntime();

        try {
            Submission<String> submission = runtime.submit(
                    "default",
                    context -> "completed"
            );

            assertTrue(submission.isAccepted());
            assertEquals(TaskStatus.QUEUED, submission.handle().status());

            runtime.start();

            assertEquals(
                    "completed",
                    submission.handle().future().get(
                            WAIT.toMillis(),
                            TimeUnit.MILLISECONDS
                    )
            );

            assertEquals(
                    TaskStatus.COMPLETED,
                    submission.handle().status()
            );
        } finally {
            runtime.close();
        }
    }

    @Test
    void shouldExecuteTaskSubmittedWhileRunning() throws Exception {
        FlowGateRuntime runtime = createRuntime();

        try {
            runtime.start();

            CountDownLatch executed = new CountDownLatch(1);

            Submission<String> submission = runtime.submit(
                    "default",
                    context -> {
                        executed.countDown();
                        return "executed";
                    }
            );

            assertTrue(submission.isAccepted());
            assertTrue(executed.await(
                    WAIT.toMillis(),
                    TimeUnit.MILLISECONDS
            ));

            assertEquals(
                    "executed",
                    submission.handle().future().get(
                            WAIT.toMillis(),
                            TimeUnit.MILLISECONDS
                    )
            );
        } finally {
            runtime.close();
        }
    }

    @Test
    void shouldRejectSubmissionWhenQueueIsFull() {
        QueueConfig queue = new QueueConfig(
                "default",
                Priority.NORMAL,
                1,
                1,
                1,
                1,
                null
        );

        FlowGateRuntime runtime = new FlowGateRuntime(
                new FlowGateConfig(1, List.of(queue)),
                Duration.ofMillis(20)
        );

        try {
            Submission<String> accepted = runtime.submit(
                    "default",
                    context -> "first"
            );

            Submission<String> rejected = runtime.submit(
                    "default",
                    context -> "second"
            );

            assertTrue(accepted.isAccepted());
            assertTrue(rejected.isRejected());

            assertEquals(
                    RejectionReason.QUEUE_FULL,
                    rejected.rejectionReason()
            );

            // The runtime has not started. Closing it cancels
            // the accepted task without executing either task.
            runtime.close();

            assertEquals(
                    TaskStatus.CANCELLED,
                    accepted.handle().status()
            );
        } finally {
            runtime.close();
        }
    }

    @Test
    void shouldCancelWaitingTasksDuringShutdown() throws Exception {
        FlowGateRuntime runtime = createRuntime();

        CountDownLatch firstTaskStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstTask = new CountDownLatch(1);

        try {
            runtime.start();

            Submission<String> running = runtime.submit(
                    "default",
                    context -> {
                        firstTaskStarted.countDown();

                        assertTrue(releaseFirstTask.await(
                                WAIT.toMillis(),
                                TimeUnit.MILLISECONDS
                        ));

                        return "finished";
                    }
            );

            assertTrue(firstTaskStarted.await(
                    WAIT.toMillis(),
                    TimeUnit.MILLISECONDS
            ));

            Submission<String> waiting = runtime.submit(
                    "default",
                    context -> "should-not-execute"
            );

            assertTrue(waiting.isAccepted());

            AtomicReference<Throwable> closeFailure =
                    new AtomicReference<>();

            Thread closer = new Thread(() -> {
                try {
                    runtime.close();
                } catch (Throwable failure) {
                    closeFailure.set(failure);
                }
            });

            closer.start();

            awaitStatus(waiting, TaskStatus.CANCELLED);

            releaseFirstTask.countDown();

            closer.join(WAIT.toMillis());

            assertFalse(closer.isAlive(), "Shutdown did not finish");
            assertNull(closeFailure.get());

            assertEquals(
                    "finished",
                    running.handle().future().get(
                            WAIT.toMillis(),
                            TimeUnit.MILLISECONDS
                    )
            );

            assertEquals(
                    TaskStatus.CANCELLED,
                    waiting.handle().status()
            );

            CompletionException exception = assertThrows(
                    CompletionException.class,
                    () -> waiting.handle().future().join()
            );

            assertInstanceOf(
                    CancellationException.class,
                    exception.getCause()
            );
        } finally {
            releaseFirstTask.countDown();
            runtime.close();
        }
    }
    @Test
    void shouldReleasePermitAndContinueAfterTaskFailure()
            throws Exception {

        FlowGateRuntime runtime = createRuntime();

        try {
            runtime.start();

            Submission<String> failed = runtime.submit(
                    "default",
                    context -> {
                        throw new IllegalStateException(
                                "Simulated workload failure"
                        );
                    }
            );

            ExecutionException exception = assertThrows(
                    ExecutionException.class,
                    () -> failed.handle().future().get(
                            WAIT.toMillis(),
                            TimeUnit.MILLISECONDS
                    )
            );

            assertInstanceOf(
                    IllegalStateException.class,
                    exception.getCause()
            );

            assertEquals(
                    TaskStatus.FAILED,
                    failed.handle().status()
            );

            // The previous task failed. The same runtime must
            // still have execution capacity for another task.
            Submission<String> succeeding = runtime.submit(
                    "default",
                    context -> "recovered"
            );

            assertTrue(succeeding.isAccepted());

            assertEquals(
                    "recovered",
                    succeeding.handle().future().get(
                            WAIT.toMillis(),
                            TimeUnit.MILLISECONDS
                    )
            );

            assertEquals(
                    TaskStatus.COMPLETED,
                    succeeding.handle().status()
            );

        } finally {
            runtime.close();
        }
    }
    @Test
    void shouldRecoverFromConcurrentWorkloadFailures()
            throws Exception {

        int taskCount = 100;

        QueueConfig queue = new QueueConfig(
                "default",
                Priority.NORMAL,
                1,
                128,
                4,
                4,
                null
        );

        FlowGateRuntime runtime = new FlowGateRuntime(
                new FlowGateConfig(4, List.of(queue)),
                Duration.ofMillis(5)
        );

        CountDownLatch completed = new CountDownLatch(taskCount);

        List<Submission<String>> submissions =
                new java.util.ArrayList<>();

        try {
            runtime.start();

            for (int i = 0; i < taskCount; i++) {
                final int taskId = i;

                Submission<String> submission = runtime.submit(
                        "default",
                        context -> {
                            try {
                                if (taskId % 2 == 0) {
                                    throw new IllegalStateException(
                                            "Simulated failure: " + taskId
                                    );
                                }

                                return "success-" + taskId;
                            } finally {
                                completed.countDown();
                            }
                        }
                );

                assertTrue(
                        submission.isAccepted(),
                        "Task should be accepted: " + taskId
                );

                submissions.add(submission);
            }

            assertTrue(
                    completed.await(15, TimeUnit.SECONDS),
                    "Not all workloads finished"
            );

            int successful = 0;
            int failed = 0;

            for (int i = 0; i < taskCount; i++) {
                Submission<String> submission = submissions.get(i);

                if (i % 2 == 0) {
                    ExecutionException exception = assertThrows(
                            ExecutionException.class,
                            () -> submission.handle().future().get(
                                    3,
                                    TimeUnit.SECONDS
                            )
                    );

                    assertInstanceOf(
                            IllegalStateException.class,
                            exception.getCause()
                    );

                    assertEquals(
                            TaskStatus.FAILED,
                            submission.handle().status()
                    );

                    failed++;
                } else {
                    assertEquals(
                            "success-" + i,
                            submission.handle().future().get(
                                    3,
                                    TimeUnit.SECONDS
                            )
                    );

                    assertEquals(
                            TaskStatus.COMPLETED,
                            submission.handle().status()
                    );

                    successful++;
                }
            }

            assertEquals(50, successful);
            assertEquals(50, failed);

            // Verify the runtime remains usable after all failures.
            Submission<String> recovery = runtime.submit(
                    "default",
                    context -> "runtime-alive"
            );

            assertTrue(recovery.isAccepted());

            assertEquals(
                    "runtime-alive",
                    recovery.handle().future().get(
                            3,
                            TimeUnit.SECONDS
                    )
            );

        } finally {
            runtime.close();
        }
    }
    @Test
    void shouldApplyQueueDefaultDeadline() throws Exception {

        QueueConfig queue = new QueueConfig(
                "default",
                Priority.NORMAL,
                1,
                10,
                0,
                1,
                Duration.ofMillis(100)
        );

        FlowGateRuntime runtime = new FlowGateRuntime(
                new FlowGateConfig(1, List.of(queue)),
                Duration.ofMillis(5)
        );

        CountDownLatch running = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        try {
            runtime.start();

            Submission<String> blocker = runtime.submit(
                    "default",
                    context -> {
                        running.countDown();
                        release.await();
                        return "finished";
                    },
                    null
            );

            assertTrue(running.await(2, TimeUnit.SECONDS));

            Submission<String> expired = runtime.submit(
                    "default",
                    context -> "should-not-run"
            );

            assertTrue(expired.isAccepted());

            ExecutionException exception = assertThrows(
                    ExecutionException.class,
                    () -> expired.handle().future().get(
                            3, TimeUnit.SECONDS
                    )
            );

            assertInstanceOf(
                    TimeoutException.class,
                    exception.getCause()
            );

            assertEquals(
                    TaskStatus.EXPIRED,
                    expired.handle().status()
            );

            release.countDown();

            assertEquals(
                    "finished",
                    blocker.handle().future().get(
                            2, TimeUnit.SECONDS
                    )
            );

        } finally {
            release.countDown();
            runtime.close();
        }
    }

    @Test
    void explicitNullDeadlineShouldOverrideQueueDefault()
            throws Exception {

        QueueConfig queue = new QueueConfig(
                "default",
                Priority.NORMAL,
                1,
                10,
                0,
                1,
                Duration.ofMillis(100)
        );

        FlowGateRuntime runtime = new FlowGateRuntime(
                new FlowGateConfig(1, List.of(queue)),
                Duration.ofMillis(5)
        );

        try {
            runtime.start();

            Submission<String> submission = runtime.submit(
                    "default",
                    context -> "completed",
                    null
            );

            assertTrue(submission.isAccepted());

            assertEquals(
                    "completed",
                    submission.handle().future().get(
                            2, TimeUnit.SECONDS
                    )
            );

            assertEquals(
                    TaskStatus.COMPLETED,
                    submission.handle().status()
            );

        } finally {
            runtime.close();
        }
    }
    @Test
    void shouldAllowRunningTaskToFinishDuringShutdown() throws Exception {
        FlowGateRuntime runtime = createRuntime();

        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);

        try {
            runtime.start();

            Submission<String> submission = runtime.submit(
                    "default",
                    context -> {
                        taskStarted.countDown();

                        assertTrue(releaseTask.await(
                                WAIT.toMillis(),
                                TimeUnit.MILLISECONDS
                        ));

                        return "finished";
                    }
            );

            assertTrue(taskStarted.await(
                    WAIT.toMillis(),
                    TimeUnit.MILLISECONDS
            ));

            AtomicReference<Throwable> closeFailure =
                    new AtomicReference<>();

            Thread closer = new Thread(() -> {
                try {
                    runtime.close();
                } catch (Throwable failure) {
                    closeFailure.set(failure);
                }
            });

            closer.start();

            awaitCondition(
                    () -> !runtime.isRunning(),
                    WAIT
            );

            releaseTask.countDown();

            closer.join(WAIT.toMillis());

            assertFalse(closer.isAlive());
            assertNull(closeFailure.get());

            assertEquals(
                    "finished",
                    submission.handle().future().get(
                            WAIT.toMillis(),
                            TimeUnit.MILLISECONDS
                    )
            );

            assertEquals(
                    TaskStatus.COMPLETED,
                    submission.handle().status()
            );
        } finally {
            releaseTask.countDown();
            runtime.close();
        }
    }

    @Test
    void shouldMakeConcurrentCloseCallsWaitForTermination()
            throws Exception {

        FlowGateRuntime runtime = createRuntime();

        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);

        AtomicReference<Throwable> firstFailure =
                new AtomicReference<>();

        AtomicReference<Throwable> secondFailure =
                new AtomicReference<>();

        try {
            runtime.start();

            runtime.submit(
                    "default",
                    context -> {
                        taskStarted.countDown();
                        releaseTask.await();
                        return "finished";
                    }
            );

            assertTrue(taskStarted.await(
                    WAIT.toMillis(),
                    TimeUnit.MILLISECONDS
            ));

            Thread firstCloser = new Thread(() -> {
                try {
                    runtime.close();
                } catch (Throwable failure) {
                    firstFailure.set(failure);
                }
            });

            Thread secondCloser = new Thread(() -> {
                try {
                    runtime.close();
                } catch (Throwable failure) {
                    secondFailure.set(failure);
                }
            });

            firstCloser.start();

            awaitCondition(
                    () -> !runtime.isRunning(),
                    WAIT
            );

            secondCloser.start();

            releaseTask.countDown();

            firstCloser.join(WAIT.toMillis());
            secondCloser.join(WAIT.toMillis());

            assertFalse(firstCloser.isAlive());
            assertFalse(secondCloser.isAlive());

            assertNull(firstFailure.get());
            assertNull(secondFailure.get());
        } finally {
            releaseTask.countDown();
            runtime.close();
        }
    }

    @Test
    void shouldNotDeadlockWhenWorkerInitiatesShutdown()
            throws Exception {

        FlowGateRuntime runtime = createRuntime();

        CountDownLatch closeReturned = new CountDownLatch(1);

        AtomicReference<FlowGateRuntime> runtimeReference =
                new AtomicReference<>(runtime);

        try {
            runtime.start();

            Submission<String> submission = runtime.submit(
                    "default",
                    context -> {
                        runtimeReference.get().close();
                        closeReturned.countDown();
                        return "worker-finished";
                    }
            );

            assertTrue(closeReturned.await(
                    WAIT.toMillis(),
                    TimeUnit.MILLISECONDS
            ));

            assertEquals(
                    "worker-finished",
                    submission.handle().future().get(
                            WAIT.toMillis(),
                            TimeUnit.MILLISECONDS
                    )
            );

            // An external caller must still be able to wait
            // for the worker executor to terminate.
            runtime.close();

            assertEquals(
                    TaskStatus.COMPLETED,
                    submission.handle().status()
            );
        } finally {
            runtime.close();
        }
    }

    @Test
    void shouldBeIdempotentWhenClosedRepeatedly() {
        FlowGateRuntime runtime = createRuntime();

        assertDoesNotThrow(() -> {
            runtime.close();
            runtime.close();
            runtime.close();
        });

        assertFalse(runtime.isRunning());
    }

    @Test
    void shouldRejectSubmissionAfterShutdown() {
        FlowGateRuntime runtime = createRuntime();

        runtime.close();

        assertThrows(
                IllegalStateException.class,
                () -> runtime.submit(
                        "default",
                        context -> "should-not-run"
                )
        );
    }

    private static void awaitStatus(
            Submission<?> submission,
            TaskStatus expected
    ) {
        awaitCondition(
                () -> submission.handle().status() == expected,
                WAIT
        );

        assertEquals(expected, submission.handle().status());
    }

    private static void awaitCondition(
            java.util.function.BooleanSupplier condition,
            Duration timeout
    ) {
        long deadline = System.nanoTime() + timeout.toNanos();

        while (!condition.getAsBoolean()) {
            if (System.nanoTime() - deadline >= 0) {
                fail("Condition was not satisfied within " + timeout);
            }

            Thread.onSpinWait();
        }
    }
}
