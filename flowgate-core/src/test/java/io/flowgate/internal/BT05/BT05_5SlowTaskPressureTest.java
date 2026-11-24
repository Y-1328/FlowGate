package io.flowgate.internal.BT05;

import io.flowgate.api.Submission;
import io.flowgate.internal.runtime.FlowGateRuntime;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class BT05_5SlowTaskPressureTest {

    @Test
    void slowTasksDoNotPermanentlyBlockExecution() throws Exception {

        int concurrency = 2;
        int taskCount = 20;

        FlowGateRuntime runtime =
                BT05TestSupport.runtime(concurrency, 100, concurrency);

        CountDownLatch slowTasksStarted = new CountDownLatch(concurrency);
        CountDownLatch releaseSlowTasks = new CountDownLatch(1);

        // FIX: Declare submissions outside the try block
        List<Submission<String>> submissions = new ArrayList<>();

        try {
            runtime.start();

            // First tasks occupy the worker slots.
            for (int i = 0; i < concurrency; i++) {
                submissions.add(runtime.submit("default", context -> {
                    slowTasksStarted.countDown();
                    releaseSlowTasks.await();
                    return "slow";
                }));
            }

            assertTrue(
                    slowTasksStarted.await(5, TimeUnit.SECONDS),
                    "Slow tasks did not occupy the workers"
            );

            // Additional tasks queue behind the slow tasks.
            for (int i = 0; i < taskCount; i++) {
                final int id = i;

                submissions.add(runtime.submit("default", context -> "task-" + id));
            }

        } finally {
            releaseSlowTasks.countDown();
        }

        try {
            // Verify all tasks eventually finish.
            for (Submission<String> submission : submissions) {
                assertNotNull(
                        submission.handle().future().get(
                                BT05TestSupport.TIMEOUT.toMillis(),
                                TimeUnit.MILLISECONDS
                        )
                );
            }
        } finally {
            runtime.close();
        }
    }
}