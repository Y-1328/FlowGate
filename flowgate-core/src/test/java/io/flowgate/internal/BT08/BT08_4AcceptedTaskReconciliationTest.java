
package io.flowgate.internal.BT08;

import io.flowgate.api.Submission;
import io.flowgate.internal.runtime.FlowGateRuntime;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class BT08_4AcceptedTaskReconciliationTest {

    @Test
    void acceptedTasksAreReconciledAfterCompletion() throws Exception {

        int totalTasks = 40;

        AtomicInteger executed = new AtomicInteger();
        List<Submission<Integer>> submissions = new ArrayList<>();

        FlowGateRuntime runtime = BT08TestSupport.runtime(
                4,
                BT08TestSupport.queue("q", 100, 4, null)
        );

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
                        "Task " + taskId + " should be accepted"
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
                    "Every accepted task should execute exactly once"
            );

        } finally {
            runtime.close();
        }
    }
}
