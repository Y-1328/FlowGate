package io.flowgate.internal.BT07;

import io.flowgate.api.Submission;
import io.flowgate.internal.runtime.FlowGateRuntime;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class BT07_8MixedRetryCancellationTest {

    @Test
    void cancellationAndRetryPressureCanCoexist() throws Exception {
        int runningTasks = 4;
        int queuedTasks = 3;

        CountDownLatch runningStarted = new CountDownLatch(runningTasks);
        CountDownLatch releaseRunning = new CountDownLatch(1);

        FlowGateRuntime runtime = BT07TestSupport.runtime(
                4,
                BT07TestSupport.queue(
                        "mixed-cancel",
                        30,
                        4,
                        BT07TestSupport.retry(2, Duration.ofMillis(5))
                )
        );

        List<Submission<Integer>> retrySubmissions = new ArrayList<>();
        List<AtomicInteger> retryAttempts = new ArrayList<>();
        List<Submission<Integer>> queuedSubmissions = new ArrayList<>();
        List<AtomicInteger> queuedAttempts = new ArrayList<>();

        runtime.start();

        try {
            for (int i = 0; i < runningTasks; i++) {
                AtomicInteger count = new AtomicInteger();
                retryAttempts.add(count);

                Submission<Integer> submission = runtime.submit(
                        "mixed-cancel",
                        context -> {
                            if (count.incrementAndGet() == 1) {
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
                                        "temporary retryable failure"
                                );
                            }

                            return 50;
                        }
                );

                retrySubmissions.add(submission);
                assertTrue(submission.isAccepted());
            }

            assertTrue(
                    runningStarted.await(5, TimeUnit.SECONDS),
                    "Running tasks did not occupy all workers"
            );

            for (int i = 0; i < queuedTasks; i++) {
                AtomicInteger count = new AtomicInteger();
                queuedAttempts.add(count);

                Submission<Integer> submission = runtime.submit(
                        "mixed-cancel",
                        context -> {
                            count.incrementAndGet();
                            return 60;
                        }
                );

                queuedSubmissions.add(submission);
                assertTrue(submission.isAccepted());
            }

            for (Submission<Integer> submission : queuedSubmissions) {
                assertTrue(
                        submission.cancel(),
                        "Expected queued task cancellation to succeed"
                );
            }

            releaseRunning.countDown();

            for (Submission<Integer> submission : retrySubmissions) {
                assertEquals(
                        50,
                        submission.future().get(10, TimeUnit.SECONDS)
                );
            }

            for (AtomicInteger count : retryAttempts) {
                assertEquals(2, count.get());
            }

            for (AtomicInteger count : queuedAttempts) {
                assertEquals(
                        0,
                        count.get(),
                        "Cancelled queued task should not have started"
                );
            }
        } finally {
            releaseRunning.countDown();
            runtime.close();
        }
    }
}