
package io.flowgate.internal.BT08;

import io.flowgate.api.Submission;
import io.flowgate.internal.runtime.FlowGateRuntime;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class BT08_1SubmitShutdownRaceTest {

    @Test
    void submissionAndShutdownRace() throws InterruptedException {

        int totalTasks = 100;

        AtomicInteger attempted = new AtomicInteger();
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        AtomicInteger closedRejections = new AtomicInteger();

        AtomicReference<Throwable> threadFailure =
                new AtomicReference<>();

        CountDownLatch firstSubmissionCompleted =
                new CountDownLatch(1);

        FlowGateRuntime runtime = BT08TestSupport.runtime(
                4,
                BT08TestSupport.queue("q", 150, 4, null)
        );

        runtime.start();

        Thread submitter = new Thread(() -> {

            try {
                for (int i = 0; i < totalTasks; i++) {

                    final int taskId = i;

                    attempted.incrementAndGet();

                    try {
                        Submission<Integer> result = runtime.submit(
                                "q",
                                context -> taskId
                        );

                        if (result.isAccepted()) {
                            accepted.incrementAndGet();
                        } else {
                            rejected.incrementAndGet();
                        }

                    } catch (IllegalStateException exception) {

                        if ("FlowGate runtime is closed"
                                .equals(exception.getMessage())) {

                            closedRejections.incrementAndGet();

                        } else {
                            throw exception;
                        }

                    } finally {
                        firstSubmissionCompleted.countDown();
                    }
                }

            } catch (Throwable failure) {
                threadFailure.set(failure);
            }

        }, "bt08-submit-shutdown");

        try {
            submitter.start();

            assertTrue(
                    firstSubmissionCompleted.await(
                            5,
                            TimeUnit.SECONDS
                    ),
                    "Submitter did not complete its first submission"
            );

            // Race the remaining submissions against shutdown.
            runtime.close();

            submitter.join(10_000);

            assertFalse(
                    submitter.isAlive(),
                    "Submitter thread did not terminate"
            );

            assertNull(
                    threadFailure.get(),
                    "Submitter thread failed"
            );

            assertEquals(
                    totalTasks,
                    attempted.get(),
                    "All submissions must be attempted"
            );

            assertEquals(
                    totalTasks,
                    accepted.get()
                            + rejected.get()
                            + closedRejections.get(),
                    "Every attempted submission must have an observable outcome"
            );

        } finally {

            if (submitter.isAlive()) {
                submitter.interrupt();
                submitter.join(2_000);
            }

            runtime.close();
        }
    }
}
