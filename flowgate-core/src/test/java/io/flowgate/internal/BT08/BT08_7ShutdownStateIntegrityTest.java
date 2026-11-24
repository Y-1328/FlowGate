
package io.flowgate.internal.BT08;

import io.flowgate.api.Submission;
import io.flowgate.internal.runtime.FlowGateRuntime;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class BT08_7ShutdownStateIntegrityTest {

    @Test
    void runtimeMaintainsConsistentTaskResultsBeforeShutdown()
            throws Exception {

        int totalTasks = 30;

        AtomicInteger executed = new AtomicInteger();

        FlowGateRuntime runtime = BT08TestSupport.runtime(
                4,
                BT08TestSupport.queue("q", 100, 4, null)
        );

        List<Submission<Integer>> submissions = new ArrayList<>();

        runtime.start();

        try {
            for (int i = 0; i < totalTasks; i++) {
                final int taskId = i;

                Submission<Integer> submission = runtime.submit(
                        "q",
                        context -> {
                            executed.incrementAndGet();
                            return taskId;
                        }
                );

                assertTrue(
                        submission.isAccepted(),
                        "Task should be accepted: " + taskId
                );

                submissions.add(submission);
            }

            for (int i = 0; i < totalTasks; i++) {
                assertEquals(
                        i,
                        submissions.get(i).future().get(
                                10,
                                TimeUnit.SECONDS
                        )
                );
            }

            assertEquals(
                    totalTasks,
                    executed.get(),
                    "Execution count must match submitted tasks"
            );

            assertEquals(
                    totalTasks,
                    submissions.size(),
                    "All accepted submissions must be accounted for"
            );

        } finally {
            runtime.close();
        }
    }
}
