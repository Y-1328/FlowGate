package io.flowgate.internal.BT05;

import io.flowgate.api.Submission;
import io.flowgate.api.TaskStatus;
import io.flowgate.internal.runtime.FlowGateRuntime;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class BT05_8MixedExecutionAbuseTest {

    @Test
    void mixedExecutionAbuseMaintainsEngineIntegrity() throws Exception {

        int taskCount = 120;

        FlowGateRuntime runtime =
                BT05TestSupport.runtime(4, 200, 4);

        AtomicInteger completed = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();

        try {
            runtime.start();

            List<Submission<Integer>> submissions = new ArrayList<>();

            for (int i = 0; i < taskCount; i++) {
                final int id = i;

                submissions.add(runtime.submit("default", context -> {

                    if (id % 5 == 0) {
                        throw new IllegalStateException("Mixed failure " + id);
                    }

                    if (id % 7 == 0) {
                        Thread.sleep(15);
                    }

                    completed.incrementAndGet();
                    return id;
                }));
            }

            for (int i = 0; i < taskCount; i++) {
                Submission<Integer> submission = submissions.get(i);

                try {
                    submission.handle().future().get(
                            BT05TestSupport.TIMEOUT.toMillis(),
                            TimeUnit.MILLISECONDS
                    );
                } catch (ExecutionException exception) {
                    failed.incrementAndGet();
                }
            }

            assertTrue(completed.get() > 0,
                    "No successful tasks were completed");

            assertTrue(failed.get() > 0,
                    "Failure pressure was not observed");

            // Recovery task after mixed workload.
            Submission<String> recovery = runtime.submit(
                    "default",
                    context -> "recovered"
            );

            assertEquals(
                    "recovered",
                    recovery.handle().future().get(
                            BT05TestSupport.TIMEOUT.toMillis(),
                            TimeUnit.MILLISECONDS
                    )
            );

            assertEquals(
                    TaskStatus.COMPLETED,
                    recovery.handle().status()
            );

        } finally {
            runtime.close();
        }
    }
}