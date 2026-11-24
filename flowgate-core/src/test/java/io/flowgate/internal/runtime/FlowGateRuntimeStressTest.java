
package io.flowgate.internal.runtime;

import io.flowgate.api.Priority;
import io.flowgate.api.Submission;
import io.flowgate.api.TaskStatus;
import io.flowgate.config.FlowGateConfig;
import io.flowgate.config.QueueConfig;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class FlowGateRuntimeStressTest {

    private static final Duration WAIT = Duration.ofSeconds(10);

    private FlowGateRuntime createRuntime() {
        QueueConfig queue = new QueueConfig(
                "default",
                Priority.NORMAL,
                1,
                1000,
                4,
                4,
                null
        );

        return new FlowGateRuntime(
                new FlowGateConfig(4, List.of(queue)),
                Duration.ofMillis(10)
        );
    }

    @Test
    void shouldHandleConcurrentSubmissionsWhileRunning()
            throws Exception {

        FlowGateRuntime runtime = createRuntime();

        int submitterCount = 8;
        int tasksPerSubmitter = 100;
        int expectedTasks = submitterCount * tasksPerSubmitter;

        List<Submission<Integer>> submissions =
                Collections.synchronizedList(new ArrayList<>());

        CountDownLatch ready = new CountDownLatch(submitterCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(expectedTasks);

        AtomicInteger executionCount = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Thread[] submitters = new Thread[submitterCount];

        try {
            runtime.start();

            for (int i = 0; i < submitterCount; i++) {
                submitters[i] = new Thread(() -> {
                    ready.countDown();

                    try {
                        assertTrue(start.await(
                                WAIT.toMillis(),
                                TimeUnit.MILLISECONDS
                        ));

                        for (int j = 0; j < tasksPerSubmitter; j++) {
                            Submission<Integer> submission =
                                    runtime.submit(
                                            "default",
                                            context -> {
                                                executionCount.incrementAndGet();
                                                completed.countDown();
                                                return 1;
                                            }
                                    );

                            if (submission.isAccepted()) {
                                submissions.add(submission);
                            } else {
                                failure.compareAndSet(
                                        null,
                                        new AssertionError(
                                                "Unexpected rejection: "
                                                        + submission.rejectionReason()
                                        )
                                );
                            }
                        }
                    } catch (Throwable exception) {
                        failure.compareAndSet(null, exception);
                    }
                });

                submitters[i].start();
            }

            assertTrue(ready.await(
                    WAIT.toMillis(),
                    TimeUnit.MILLISECONDS
            ));

            start.countDown();

            for (Thread submitter : submitters) {
                submitter.join(WAIT.toMillis());
                assertFalse(submitter.isAlive(),
                        "Submitter thread did not finish");
            }

            assertNull(failure.get());

            assertEquals(expectedTasks, submissions.size());

            assertTrue(completed.await(
                    WAIT.toMillis(),
                    TimeUnit.MILLISECONDS
            ));

            assertEquals(expectedTasks, executionCount.get());

            for (Submission<Integer> submission : submissions) {
                // The task body counts the latch down before the worker records
                // COMPLETED, so wait on the future first: it is completed only
                // after the state transition. The assertions are unchanged.
                assertEquals(1, submission.handle().future().get(
                        WAIT.toMillis(),
                        TimeUnit.MILLISECONDS
                ));

                assertTrue(submission.handle().future().isDone());

                assertEquals(
                        TaskStatus.COMPLETED,
                        submission.handle().status()
                );
            }
        } finally {
            start.countDown();
            runtime.close();
        }
    }

    @Test
    void shouldNotStrandAcceptedTasksDuringConcurrentShutdown()
            throws Exception {

        FlowGateRuntime runtime = createRuntime();

        int submitterCount = 8;
        int tasksPerSubmitter = 500;

        List<Submission<Integer>> accepted =
                Collections.synchronizedList(new ArrayList<>());

        CountDownLatch ready = new CountDownLatch(submitterCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch firstAccepted = new CountDownLatch(1);

        AtomicInteger executed = new AtomicInteger();
        AtomicReference<Throwable> unexpectedFailure =
                new AtomicReference<>();

        Thread[] submitters = new Thread[submitterCount];

        Thread closer = new Thread(runtime::close);

        try {
            runtime.start();

            for (int i = 0; i < submitterCount; i++) {
                submitters[i] = new Thread(() -> {
                    ready.countDown();

                    try {
                        assertTrue(start.await(
                                WAIT.toMillis(),
                                TimeUnit.MILLISECONDS
                        ));

                        for (int j = 0; j < tasksPerSubmitter; j++) {
                            try {
                                Submission<Integer> submission =
                                        runtime.submit(
                                                "default",
                                                context -> {
                                                    executed.incrementAndGet();
                                                    return 1;
                                                }
                                        );

                                if (submission.isAccepted()) {
                                    accepted.add(submission);
                                    firstAccepted.countDown();
                                }
                            } catch (IllegalStateException closed) {
                                // Runtime shutdown won the lifecycle race.
                                break;
                            }
                        }
                    } catch (Throwable exception) {
                        unexpectedFailure.compareAndSet(
                                null,
                                exception
                        );
                    }
                });

                submitters[i].start();
            }

            assertTrue(ready.await(
                    WAIT.toMillis(),
                    TimeUnit.MILLISECONDS
            ));

            start.countDown();

            assertTrue(firstAccepted.await(
                    WAIT.toMillis(),
                    TimeUnit.MILLISECONDS
            ));

            // Shutdown races with the remaining submissions.
            closer.start();

            for (Thread submitter : submitters) {
                submitter.join(WAIT.toMillis());

                assertFalse(
                        submitter.isAlive(),
                        "Submitter did not finish"
                );
            }

            closer.join(WAIT.toMillis());

            assertFalse(
                    closer.isAlive(),
                    "Runtime shutdown did not finish"
            );

            assertNull(unexpectedFailure.get());

            assertFalse(accepted.isEmpty());

            // Every accepted task must have reached a terminal state.
            for (Submission<Integer> submission : accepted) {
                TaskStatus status = submission.handle().status();

                assertTrue(
                        status.isTerminal(),
                        "Accepted task was stranded in status " + status
                );

                assertTrue(
                        submission.handle().future().isDone(),
                        "Accepted task future was left incomplete"
                );
            }

            // No task can execute more than once.
            assertTrue(executed.get() <= accepted.size());
        } finally {
            start.countDown();

            if (closer.isAlive()) {
                closer.join(WAIT.toMillis());
            }

            runtime.close();
        }
    }
}
