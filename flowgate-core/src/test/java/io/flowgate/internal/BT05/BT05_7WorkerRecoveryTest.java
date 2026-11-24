package io.flowgate.internal.BT05;

import io.flowgate.api.Submission;
import io.flowgate.api.TaskStatus;
import io.flowgate.internal.runtime.FlowGateRuntime;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class BT05_7WorkerRecoveryTest {

    @Test
    void workersRecoverAfterRepeatedTaskFailures() throws Exception {

        FlowGateRuntime runtime =
                BT05TestSupport.runtime(2, 100, 2);

        try {
            runtime.start();

            // Failure phase.
            for (int i = 0; i < 30; i++) {
                Submission<Integer> failed = runtime.submit("default", context -> {
                    throw new RuntimeException("Failure injection");
                });

                assertThrows(
                        ExecutionException.class,
                        () -> failed.handle().future().get(
                                BT05TestSupport.TIMEOUT.toMillis(),
                                TimeUnit.MILLISECONDS
                        )
                );
            }

            // Recovery phase.
            for (int i = 0; i < 30; i++) {
                final int expected = i;

                Submission<Integer> recovered = runtime.submit(
                        "default",
                        context -> expected
                );

                assertEquals(
                        expected,
                        recovered.handle().future().get(
                                BT05TestSupport.TIMEOUT.toMillis(),
                                TimeUnit.MILLISECONDS
                        )
                );

                assertEquals(
                        TaskStatus.COMPLETED,
                        recovered.handle().status()
                );
            }

        } finally {
            runtime.close();
        }
    }
}