
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

class BT08_3MultiWorkloadShutdownRaceTest {

    @Test
    void multiQueueSubmissionDuringShutdown() throws InterruptedException {

        int tasksPerQueue = 50;
        int totalTasks = tasksPerQueue * 2;

        AtomicInteger attempted = new AtomicInteger();
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        AtomicInteger closedRejections = new AtomicInteger();

        CountDownLatch firstSubmissionCompleted =
                new CountDownLatch(2);

        AtomicReference<Throwable> threadFailure =
                new AtomicReference<>();

        FlowGateRuntime runtime = BT08TestSupport.runtime(
                4,
                BT08TestSupport.queue("queue-a", 100, 2, null),
                BT08TestSupport.queue("queue-b", 100, 2, null)
        );

        List<Thread> submitters = new ArrayList<>();

        runtime.start();

        try {
            String[] queueIds = {"queue-a", "queue-b"};

            for (int queueIndex = 0;
                 queueIndex < queueIds.length;
                 queueIndex++) {

                final String queueId = queueIds[queueIndex];
                final int queueIndexValue = queueIndex;

                Thread submitter = new Thread(() -> {
                    try {
                        for (int i = 0; i < tasksPerQueue; i++) {

                            final int taskId =
                                    queueIndexValue * tasksPerQueue + i;

                            attempted.incrementAndGet();

                            try {
                                Submission<Integer> result = runtime.submit(
                                        queueId,
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
                }, "bt08-" + queueId);

                submitters.add(submitter);
            }

            for (Thread submitter : submitters) {
                submitter.start();
            }

            assertTrue(
                    firstSubmissionCompleted.await(
                            5,
                            TimeUnit.SECONDS
                    ),
                    "Both queues must complete their first submission"
            );

            runtime.close();

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
                    "Not all submission attempts were executed"
            );

            assertEquals(
                    totalTasks,
                    accepted.get()
                            + rejected.get()
                            + closedRejections.get(),
                    "Every submission must have an observable outcome"
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
