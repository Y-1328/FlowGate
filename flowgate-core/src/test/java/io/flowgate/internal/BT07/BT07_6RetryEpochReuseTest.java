
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

class BT07_6RetryEpochReuseTest {

    @Test
    void retryStateIsIsolatedAcrossSuccessiveBatches() throws Exception {
        int batches = 3;
        int tasksPerBatch = 5;

        FlowGateRuntime runtime = BT07TestSupport.runtime(
                4,
                BT07TestSupport.queue(
                        "epoch",
                        50,
                        4,
                        BT07TestSupport.retry(2, Duration.ofMillis(5))
                )
        );

        runtime.start();

        try {
            for (int batch = 0; batch < batches; batch++) {

                // Capture the current batch value for the lambda.
                final int batchIndex = batch;

                List<Submission<Integer>> submissions = new ArrayList<>();
                List<AtomicInteger> attempts = new ArrayList<>();

                for (int task = 0; task < tasksPerBatch; task++) {
                    AtomicInteger count = new AtomicInteger();
                    attempts.add(count);

                    final int expectedValue =
                            batchIndex * tasksPerBatch + task;

                    Submission<Integer> submission = runtime.submit(
                            "epoch",
                            context -> {
                                if (count.incrementAndGet() == 1) {
                                    throw new IOException(
                                            "batch " + batchIndex
                                                    + " temporary failure"
                                    );
                                }

                                return expectedValue;
                            }
                    );

                    submissions.add(submission);
                    assertTrue(submission.isAccepted());
                }

                for (int task = 0; task < tasksPerBatch; task++) {
                    assertEquals(
                            batchIndex * tasksPerBatch + task,
                            submissions.get(task).future().get(
                                    10,
                                    TimeUnit.SECONDS
                            )
                    );

                    assertEquals(2, attempts.get(task).get());
                }
            }
        } finally {
            runtime.close();
        }
    }
}
