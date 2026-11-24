
package io.flowgate.internal.BT08;

import io.flowgate.api.Submission;
import io.flowgate.internal.runtime.FlowGateRuntime;
import io.flowgate.policy.RetryPolicy;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class BT08_8MixedLifecycleAdversarialTest {

    @Test
    void mixedRetryAndCancellationPressure() throws Exception {

        int runningTasks = 2;
        int queuedTasks = 5;

        CountDownLatch runningStarted =
                new CountDownLatch(runningTasks);

        CountDownLatch releaseRunning =
                new CountDownLatch(1);

        List<Submission<Integer>> runningSubmissions =
                new ArrayList<>();

        List<Submission<Integer>> queuedSubmissions =
                new ArrayList<>();

        List<AtomicInteger> runningAttempts =
                new ArrayList<>();

        List<AtomicInteger> queuedExecutions =
                new ArrayList<>();

        RetryPolicy retryPolicy = RetryPolicy.builder()
                .maxAttempts(2)
                .retryOn(IOException.class)
                .initialBackoff(Duration.ofMillis(5))
                .maxBackoff(Duration.ofMillis(20))
                .multiplier(2.0)
                .build();

        FlowGateRuntime runtime = BT08TestSupport.runtime(
                2,
                BT08TestSupport.queue(
                        "mixed",
                        30,
                        2,
                        retryPolicy
                )
        );

        runtime.start();

        try {
            // Occupy both workers with retryable workloads.
            for (int i = 0; i < runningTasks; i++) {

                AtomicInteger attempts = new AtomicInteger();
                runningAttempts.add(attempts);

                Submission<Integer> submission = runtime.submit(
                        "mixed",
                        context -> {

                            if (attempts.incrementAndGet() == 1) {

                                runningStarted.countDown();

                                if (!releaseRunning.await(
                                        5,
                                        TimeUnit.SECONDS
                                )) {
                                    throw new IllegalStateException(
                                            "Timed out waiting for release"
                                    );
                                }

                                throw new IOException(
                                        "Simulated retryable failure"
                                );
                            }

                            return 77;
                        }
                );

                assertTrue(
                        submission.isAccepted(),
                        "Running task should be accepted"
                );

                runningSubmissions.add(submission);
            }

            // Ensure both workers are occupied.
            assertTrue(
                    runningStarted.await(5, TimeUnit.SECONDS),
                    "Both running workloads should occupy the workers"
            );

            // Submit additional tasks while both workers are occupied.
            for (int i = 0; i < queuedTasks; i++) {

                AtomicInteger executions = new AtomicInteger();
                queuedExecutions.add(executions);

                Submission<Integer> submission = runtime.submit(
                        "mixed",
                        context -> {
                            executions.incrementAndGet();
                            return 99;
                        }
                );

                assertTrue(
                        submission.isAccepted(),
                        "Queued task should be accepted"
                );

                queuedSubmissions.add(submission);
            }

            // Cancel tasks while the workers remain occupied.
            for (Submission<Integer> submission : queuedSubmissions) {

                assertTrue(
                        submission.cancel(),
                        "Cancellation of a queued task should succeed"
                );
            }

            // Allow the running tasks to fail and retry.
            releaseRunning.countDown();

            // Verify successful recovery.
            for (Submission<Integer> submission : runningSubmissions) {

                assertEquals(
                        77,
                        submission.future().get(
                                10,
                                TimeUnit.SECONDS
                        )
                );
            }

            // Each running workload should execute twice.
            for (AtomicInteger attempts : runningAttempts) {
                assertEquals(2, attempts.get());
            }

            // Cancelled queued tasks must never execute.
            for (AtomicInteger executions : queuedExecutions) {

                assertEquals(
                        0,
                        executions.get(),
                        "Cancelled queued task must not execute"
                );
            }

        } finally {
            // Prevent blocked worker tasks if an assertion fails.
            releaseRunning.countDown();
            runtime.close();
        }
    }
}
