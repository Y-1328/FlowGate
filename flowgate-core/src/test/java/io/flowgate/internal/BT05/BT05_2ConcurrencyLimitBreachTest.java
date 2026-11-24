package io.flowgate.internal.BT05;

import io.flowgate.api.Submission;
import io.flowgate.internal.runtime.FlowGateRuntime;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class BT05_2ConcurrencyLimitBreachTest {

    @Test
    void concurrentSubmissionCannotBreachWorkerLimit() throws Exception {

        int concurrency = 3;
        int taskCount = 60;

        FlowGateRuntime runtime =
                BT05TestSupport.runtime(concurrency, 100, concurrency);

        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximum = new AtomicInteger();
        CountDownLatch completed = new CountDownLatch(taskCount);

        try {
            runtime.start();

            List<Submission<Integer>> submissions = new ArrayList<>();

            for (int i = 0; i < taskCount; i++) {
                submissions.add(runtime.submit("default", context -> {
                    int current = active.incrementAndGet();
                    maximum.accumulateAndGet(current, Math::max);

                    try {
                        Thread.sleep(10);
                        return current;
                    } finally {
                        active.decrementAndGet();
                        completed.countDown();
                    }
                }));
            }

            assertTrue(completed.await(10, TimeUnit.SECONDS),
                    "Tasks did not complete in time");

            assertTrue(maximum.get() <= concurrency,
                    "Concurrency breach detected: " + maximum.get());

        } finally {
            runtime.close();
        }
    }
}