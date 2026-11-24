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

class BT07_5MultiWorkloadPressureTest {

    @Test
    void retriesAcrossQueuesCompleteSuccessfully() throws Exception {
        int tasksPerQueue = 10;

        FlowGateRuntime runtime = BT07TestSupport.runtime(
                6,
                BT07TestSupport.queue(
                        "queue-a",
                        50,
                        3,
                        BT07TestSupport.retry(2, Duration.ofMillis(5))
                ),
                BT07TestSupport.queue(
                        "queue-b",
                        50,
                        3,
                        BT07TestSupport.retry(2, Duration.ofMillis(5))
                )
        );

        List<Submission<Integer>> submissions = new ArrayList<>();
        List<AtomicInteger> attempts = new ArrayList<>();

        runtime.start();

        try {
            for (int i = 0; i < tasksPerQueue; i++) {
                AtomicInteger countA = new AtomicInteger();
                AtomicInteger countB = new AtomicInteger();

                attempts.add(countA);
                attempts.add(countB);

                Submission<Integer> a = runtime.submit(
                        "queue-a",
                        context -> {
                            if (countA.incrementAndGet() == 1) {
                                throw new IOException("queue-a failure");
                            }
                            return 1;
                        }
                );

                Submission<Integer> b = runtime.submit(
                        "queue-b",
                        context -> {
                            if (countB.incrementAndGet() == 1) {
                                throw new IOException("queue-b failure");
                            }
                            return 2;
                        }
                );

                submissions.add(a);
                submissions.add(b);

                assertTrue(a.isAccepted());
                assertTrue(b.isAccepted());
            }

            for (int i = 0; i < submissions.size(); i++) {
                int expected = i % 2 == 0 ? 1 : 2;

                assertEquals(
                        expected,
                        submissions.get(i).future().get(
                                10,
                                TimeUnit.SECONDS
                        )
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