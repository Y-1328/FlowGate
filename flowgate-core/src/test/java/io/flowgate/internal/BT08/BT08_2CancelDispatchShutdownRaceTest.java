
package io.flowgate.internal.BT08;

import io.flowgate.api.Submission;
import io.flowgate.internal.runtime.FlowGateRuntime;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class BT08_2CancelDispatchShutdownRaceTest {

    @Test
    void concurrentSubmissionDuringShutdown()
            throws InterruptedException {

        int submitterCount = 3;
        int tasksPerSubmitter = 50;
        int totalTasks = submitterCount * tasksPerSubmitter;

        AtomicInteger attempted = new AtomicInteger();
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        AtomicInteger closedRejections = new AtomicInteger();

        AtomicReference<Throwable> threadFailure =
                new AtomicReference<>();

        CountDownLatch firstSubmissionCompleted =
                new CountDownLatch(submitterCount);

        FlowGateRuntime runtime = BT08TestSupport.runtime(
                4,
                BT08TestSupport.queue("q", 200, 4, null)
        );

        List<Thread> submitters = new ArrayList<>();

        runtime.start();

        try {

            for (int threadIndex = 0;
                 threadIndex < submitterCount;
                 threadIndex++) {

                final int submitterId = threadIndex;

                Thread submitter = new Thread(() -> {

                    try {
                        for (int i = 0; i < tasksPerSubmitter; i++) {

                            final int taskId =
                                    submitterId * tasksPerSubmitter + i;

                            attempted.incrementAndGet();

                            try {
                                Submission<Integer> result =
                                        runtime.submit(
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

                                if (i == 0) {
                                    firstSubmissionCompleted.countDown();
                                }
                            }
                        }

                    } catch (Throwable failure) {
                        threadFailure.compareAndSet(null, failure);
                    }

                }, "bt08-submitter-" + threadIndex);

                submitters.add(submitter);
            }

            // Start all submitters.
            for (Thread submitter : submitters) {
                submitter.start();
            }

            // Ensure every submitter has attempted its first task.
            assertTrue(
                    firstSubmissionCompleted.await(
                            5,
                            TimeUnit.SECONDS
                    ),
                    "Not all submitter threads completed their first submission"
            );

            // Shutdown races with the remaining submissions.
            runtime.close();

            // Wait for all submitters.
            for (Thread submitter : submitters) {

                submitter.join(10_000);

                assertFalse(
                        submitter.isAlive(),
                        "Submitter did not terminate: "
                                + submitter.getName()
                );
            }

            assertNull(
                    threadFailure.get(),
                    "A submitter thread failed"
            );

            assertEquals(
                    totalTasks,
                    attempted.get(),
                    "All submission attempts must be executed"
            );

            assertEquals(
                    totalTasks,
                    accepted.get()
                            + rejected.get()
                            + closedRejections.get(),
                    "Every attempted submission must have an observable outcome"
            );

        } finally {

            for (Thread submitter : submitters) {
                if (submitter.isAlive()) {
                    submitter.interrupt();
                    submitter.join(2_000);
                }
            }

            runtime.close();
        }
    }
}
