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

class BT07_7SynchronizedFailureTest {

    @Test
    void simultaneousFailuresRecoverWithoutLosingTasks() throws Exception {
        int taskCount = 4;

        CountDownLatch allStarted = new CountDownLatch(taskCount);
        CountDownLatch release = new CountDownLatch(1);

        FlowGateRuntime runtime = BT07TestSupport.runtime(
                4,
                BT07TestSupport.queue(
                        "synchronized",
                        30,
                        4,
                        BT07TestSupport.retry(2, Duration.ofMillis(5))
                )
        );

        List<Submission<Integer>> submissions = new ArrayList<>();
        List<AtomicInteger> attempts = new ArrayList<>();

        runtime.start();

        try {
            for (int i = 0; i < taskCount; i++) {
                AtomicInteger count = new AtomicInteger();
                attempts.add(count);

                Submission<Integer> submission = runtime.submit(
                        "synchronized",
                        context -> {
                            if (count.incrementAndGet() == 1) {
                                allStarted.countDown();

                                if (!release.await(5, TimeUnit.SECONDS)) {
                                    throw new IllegalStateException(
                                            "Failure synchronization timed out"
                                    );
                                }

                                throw new IOException(
                                        "synchronized simulated failure"
                                );
                            }

                            return 100;
                        }
                );

                submissions.add(submission);
                assertTrue(submission.isAccepted());
            }

            assertTrue(
                    allStarted.await(5, TimeUnit.SECONDS),
                    "Not all tasks reached their first attempt"
            );

            release.countDown();

            for (Submission<Integer> submission : submissions) {
                assertEquals(
                        100,
                        submission.future().get(10, TimeUnit.SECONDS)
                );
            }

            for (AtomicInteger count : attempts) {
                assertEquals(2, count.get());
            }
        } finally {
            release.countDown();
            runtime.close();
        }
    }
}