package io.flowgate.internal.BT07;

import io.flowgate.api.Submission;
import io.flowgate.internal.runtime.FlowGateRuntime;
import io.flowgate.policy.RetryPolicy;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class BT07_1RetryStormTest {

    @Test
    void manyConcurrentFailuresRecoverThroughRetries() throws Exception {
        int taskCount = 24;

        FlowGateRuntime runtime = BT07TestSupport.runtime(
                4,
                BT07TestSupport.queue(
                        "storm",
                        100,
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

                int value = i;

                Submission<Integer> submission = runtime.submit(
                        "storm",
                        context -> {
                            if (count.incrementAndGet() == 1) {
                                throw new IOException("simulated first failure");
                            }
                            return value;
                        }
                );

                submissions.add(submission);
                assertTrue(submission.isAccepted());
            }

            for (Submission<Integer> submission : submissions) {
                assertEquals(
                        submission.id(),
                        submission.future().get(10, TimeUnit.SECONDS) >= 0
                                ? submission.id()
                                : -1
                );
            }

            for (AtomicInteger count : attempts) {
                assertEquals(2, count.get());
            }
        } finally {
            runtime.close();
        }
    }
}