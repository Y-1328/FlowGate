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

class BT07_4NormalAdmissionInterferenceTest {

    @Test
    void normalTasksCanBeAdmittedAlongsideRetryingTasks() throws Exception {
        int retryTaskCount = 4;
        int normalTaskCount = 8;

        CountDownLatch firstAttemptsStarted =
                new CountDownLatch(retryTaskCount);
        CountDownLatch releaseFailures = new CountDownLatch(1);

        FlowGateRuntime runtime = BT07TestSupport.runtime(
                4,
                BT07TestSupport.queue(
                        "mixed",
                        100,
                        4,
                        BT07TestSupport.retry(2, Duration.ofMillis(5))
                )
        );

        List<Submission<Integer>> retrySubmissions = new ArrayList<>();
        List<Submission<Integer>> normalSubmissions = new ArrayList<>();
        List<AtomicInteger> attempts = new ArrayList<>();

        runtime.start();

        try {
            for (int i = 0; i < retryTaskCount; i++) {
                AtomicInteger count = new AtomicInteger();
                attempts.add(count);

                Submission<Integer> submission = runtime.submit(
                        "mixed",
                        context -> {
                            if (count.incrementAndGet() == 1) {
                                firstAttemptsStarted.countDown();

                                if (!releaseFailures.await(
                                        5,
                                        TimeUnit.SECONDS
                                )) {
                                    throw new IllegalStateException(
                                            "Timed out waiting to release failure"
                                    );
                                }

                                throw new IOException("temporary failure");
                            }

                            return 10;
                        }
                );

                retrySubmissions.add(submission);
                assertTrue(submission.isAccepted());
            }

            assertTrue(
                    firstAttemptsStarted.await(5, TimeUnit.SECONDS),
                    "Initial retry attempts did not all start"
            );

            for (int i = 0; i < normalTaskCount; i++) {
                Submission<Integer> submission = runtime.submit(
                        "mixed",
                        context -> 20
                );

                normalSubmissions.add(submission);
                assertTrue(
                        submission.isAccepted(),
                        "Normal workload was rejected during retry pressure"
                );
            }

            releaseFailures.countDown();

            for (Submission<Integer> submission : retrySubmissions) {
                assertEquals(
                        10,
                        submission.future().get(10, TimeUnit.SECONDS)
                );
            }

            for (Submission<Integer> submission : normalSubmissions) {
                assertEquals(
                        20,
                        submission.future().get(10, TimeUnit.SECONDS)
                );
            }

            for (AtomicInteger count : attempts) {
                assertEquals(2, count.get());
            }
        } finally {
            releaseFailures.countDown();
            runtime.close();
        }
    }
}