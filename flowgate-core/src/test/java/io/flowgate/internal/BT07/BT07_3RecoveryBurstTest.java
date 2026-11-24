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

class BT07_3RecoveryBurstTest {

    @Test
    void failedTasksRecoverInABurst() throws Exception {
        int taskCount = 20;

        FlowGateRuntime runtime = BT07TestSupport.runtime(
                4,
                BT07TestSupport.queue(
                        "recovery",
                        100,
                        4,
                        BT07TestSupport.retry(2, Duration.ofMillis(10))
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
                        "recovery",
                        context -> {
                            if (count.incrementAndGet() == 1) {
                                throw new IOException("temporary outage");
                            }
                            return 1;
                        }
                );

                submissions.add(submission);
                assertTrue(submission.isAccepted());
            }

            for (Submission<Integer> submission : submissions) {
                assertEquals(
                        1,
                        submission.future().get(10, TimeUnit.SECONDS)
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