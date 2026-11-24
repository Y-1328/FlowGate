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

class BT05_4ExceptionStormTest {

    @Test
    void exceptionStormDoesNotBreakExecutionEngine() throws Exception {

        int taskCount = 100;
        FlowGateRuntime runtime =
                BT05TestSupport.runtime(4, 200, 4);

        AtomicInteger successfulTasks = new AtomicInteger();

        try {
            runtime.start();

            List<Submission<Integer>> submissions = new ArrayList<>();

            for (int i = 0; i < taskCount; i++) {
                final int id = i;

                submissions.add(runtime.submit("default", context -> {
                    if (id % 2 == 0) {
                        throw new IllegalStateException("BT05 failure " + id);
                    }

                    successfulTasks.incrementAndGet();
                    return id;
                }));
            }

            for (int i = 0; i < taskCount; i++) {
                Submission<Integer> submission = submissions.get(i);

                if (i % 2 == 0) {
                    assertThrows(
                            ExecutionException.class,
                            () -> submission.handle().future().get(
                                    BT05TestSupport.TIMEOUT.toMillis(),
                                    TimeUnit.MILLISECONDS
                            )
                    );

                    assertEquals(TaskStatus.FAILED,
                            submission.handle().status());
                } else {
                    assertEquals(
                            i,
                            submission.handle().future().get(
                                    BT05TestSupport.TIMEOUT.toMillis(),
                                    TimeUnit.MILLISECONDS
                            )
                    );
                }
            }

            assertEquals(taskCount / 2, successfulTasks.get());

        } finally {
            runtime.close();
        }
    }
}