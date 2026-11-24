package io.flowgate.internal.BT05;

import io.flowgate.api.Submission;
import io.flowgate.internal.runtime.FlowGateRuntime;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class BT05_6CancellationDuringExecutionTest {

    @Test
    void runningTaskObservesCancellationRequest() throws Exception {

        FlowGateRuntime runtime =
                BT05TestSupport.runtime(1, 20, 1);

        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch cancellationObserved = new CountDownLatch(1);
        AtomicBoolean observed = new AtomicBoolean();

        try {
            runtime.start();

            Submission<String> submission = runtime.submit("default", context -> {
                started.countDown();

                while (!context.isCancellationRequested()) {
                    Thread.sleep(5);
                }

                observed.set(true);
                cancellationObserved.countDown();
                return "cancel-observed";
            });

            assertTrue(started.await(5, TimeUnit.SECONDS),
                    "Task did not start");

            submission.handle().cancel();

            assertTrue(cancellationObserved.await(5, TimeUnit.SECONDS),
                    "Running task did not observe cancellation");

            assertTrue(observed.get(),
                    "Task did not exit through cancellation observation");

        } finally {
            runtime.close();
        }
    }
}