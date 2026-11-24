package io.flowgate.internal.BT05;

import io.flowgate.api.Submission;
import io.flowgate.api.TaskStatus;
import io.flowgate.internal.runtime.FlowGateRuntime;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class BT05_3DuplicateExecutionTest {

    @Test
    void logicalTaskMustNotExecuteMoreThanOnce() throws Exception {

        int taskCount = 100;

        FlowGateRuntime runtime =
                BT05TestSupport.runtime(4, 200, 4);

        AtomicInteger[] executions = new AtomicInteger[taskCount];

        for (int i = 0; i < taskCount; i++) {
            executions[i] = new AtomicInteger();
        }

        try {
            runtime.start();

            List<Submission<Integer>> submissions = new ArrayList<>();

            for (int i = 0; i < taskCount; i++) {
                final int id = i;

                submissions.add(runtime.submit("default", context -> {
                    return executions[id].incrementAndGet();
                }));
            }

            for (Submission<Integer> submission : submissions) {
                submission.handle().future().get(
                        BT05TestSupport.TIMEOUT.toMillis(),
                        TimeUnit.MILLISECONDS
                );
            }

            for (int i = 0; i < taskCount; i++) {
                assertEquals(1, executions[i].get(),
                        "Task " + i + " executed more than once");

                assertEquals(TaskStatus.COMPLETED,
                        submissions.get(i).handle().status());
            }

        } finally {
            runtime.close();
        }
    }
}