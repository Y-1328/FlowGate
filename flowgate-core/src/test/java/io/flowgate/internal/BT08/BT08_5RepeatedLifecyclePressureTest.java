
package io.flowgate.internal.BT08;

import io.flowgate.api.Submission;
import io.flowgate.internal.runtime.FlowGateRuntime;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class BT08_5RepeatedLifecyclePressureTest {

    @Test
    void repeatedRuntimeLifecycleUnderSubmissionPressure()
            throws Exception {

        int lifecycleCycles = 20;
        int tasksPerCycle = 10;

        AtomicInteger totalExecuted = new AtomicInteger();

        for (int cycle = 0; cycle < lifecycleCycles; cycle++) {

            final int cycleIndex = cycle;

            FlowGateRuntime runtime = BT08TestSupport.runtime(
                    2,
                    BT08TestSupport.queue("q", 30, 2, null)
            );

            List<Submission<Integer>> submissions = new ArrayList<>();

            runtime.start();

            try {
                for (int task = 0; task < tasksPerCycle; task++) {
                    final int taskId = task;

                    Submission<Integer> submission = runtime.submit(
                            "q",
                            context -> {
                                totalExecuted.incrementAndGet();
                                return taskId;
                            }
                    );

                    assertTrue(
                            submission.isAccepted(),
                            "Submission rejected in lifecycle cycle "
                                    + cycleIndex
                    );

                    submissions.add(submission);
                }

                for (int task = 0; task < tasksPerCycle; task++) {
                    assertEquals(
                            task,
                            submissions.get(task).future().get(
                                    10,
                                    TimeUnit.SECONDS
                            )
                    );
                }

                assertEquals(
                        tasksPerCycle,
                        submissions.size()
                );

            } finally {
                runtime.close();
            }
        }

        assertEquals(
                lifecycleCycles * tasksPerCycle,
                totalExecuted.get(),
                "All tasks across all runtime lifecycles must execute"
        );
    }
}
