package io.flowgate.internal.BT08;

import io.flowgate.api.Submission;
import io.flowgate.internal.runtime.FlowGateRuntime;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class BT08_6ConcurrentLifecycleEpochTest {

    @Test
    void concurrentSubmittersShareRuntime() throws InterruptedException {

        int threadCount = 8;
        int tasksPerThread = 50;
        int totalTasks = threadCount * tasksPerThread;

        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();

        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);

        try (FlowGateRuntime runtime = BT08TestSupport.runtime(
                8,
                BT08TestSupport.queue("q", 500, 8, null)
        )) {
            runtime.start();

            Thread[] threads = new Thread[threadCount];

            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;

                threads[t] = new Thread(() -> {
                    ready.countDown();

                    try {
                        start.await();

                        for (int i = 0; i < tasksPerThread; i++) {
                            final int taskId =
                                    threadId * tasksPerThread + i;

                            Submission<Integer> result = runtime.submit(
                                    "q",
                                    context -> taskId
                            );

                            if (result.isAccepted()) {
                                accepted.incrementAndGet();
                            } else {
                                rejected.incrementAndGet();
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }

            for (Thread thread : threads) {
                thread.start();
            }

            assertTrue(
                    ready.await(5, TimeUnit.SECONDS),
                    "Threads failed to reach ready state"
            );

            start.countDown();

            for (Thread thread : threads) {
                thread.join(15_000);

                assertFalse(
                        thread.isAlive(),
                        "Submitter thread failed to terminate"
                );
            }
        }

        assertEquals(
                totalTasks,
                accepted.get() + rejected.get()
        );
    }
}