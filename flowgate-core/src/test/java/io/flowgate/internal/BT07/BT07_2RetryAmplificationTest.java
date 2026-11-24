package io.flowgate.internal.BT07;

import io.flowgate.api.Submission;
import io.flowgate.internal.runtime.FlowGateRuntime;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class BT07_2RetryAmplificationTest {

    @Test
    void retriesRemainWithinConfiguredAttemptLimit() throws Exception {
        int taskCount = 12;
        int maxAttempts = 3;

        FlowGateRuntime runtime = BT07TestSupport.runtime(
                4,
                BT07TestSupport.queue(
                        "amplification",
                        100,
                        4,
                        BT07TestSupport.retry(
                                maxAttempts,
                                Duration.ofMillis(5)
                        )
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
                        "amplification",
                        context -> {
                            int attempt = count.incrementAndGet();

                            if (attempt < maxAttempts) {
                                throw new IOException("temporary failure");
                            }

                            return attempt;
                        }
                );

                submissions.add(submission);
                assertTrue(submission.isAccepted());
            }

            for (Submission<Integer> submission : submissions) {
                assertEquals(
                        maxAttempts,
                        submission.future().get(10, TimeUnit.SECONDS)
                );
            }

            int totalAttempts = 0;

            for (AtomicInteger count : attempts) {
                assertEquals(maxAttempts, count.get());
                totalAttempts += count.get();
            }

            assertEquals(taskCount * maxAttempts, totalAttempts);
        } finally {
            runtime.close();
        }
    }
}