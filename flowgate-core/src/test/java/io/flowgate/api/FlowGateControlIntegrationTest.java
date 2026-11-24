package io.flowgate.api;


import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class FlowGateControlIntegrationTest {

    @Test
    void cancelsAcceptedTaskThroughPublicApi() throws Exception {

        FlowGate gate = FlowGate.builder()
                .globalConcurrency(1)
                .queue("default", q -> q.capacity(10))
                .build();

        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch releaseBlocker = new CountDownLatch(1);
        AtomicBoolean cancelledTaskExecuted = new AtomicBoolean(false);

        try {
            gate.start();

            // Occupy the only worker.
            Submission<Void> blocker = gate.submit("default", () -> {
                blockerStarted.countDown();

                try {
                    releaseBlocker.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            assertTrue(blockerStarted.await(3, TimeUnit.SECONDS));
            assertTrue(blocker.isAccepted());

            // This task will remain queued.
            Submission<Void> queuedTask = gate.submit("default", () -> {
                cancelledTaskExecuted.set(true);
            });

            assertTrue(queuedTask.isAccepted());

            TaskHandle<Void> handle = queuedTask.handle();

            // Cancel through the public TaskHandle.
            assertTrue(handle.cancel());

            assertEquals(TaskStatus.CANCELLED, handle.status());

            releaseBlocker.countDown();

            blocker.handle().future().get(5, TimeUnit.SECONDS);

            assertFalse(
                    cancelledTaskExecuted.get(),
                    "Cancelled task must not execute"
            );

        } finally {
            releaseBlocker.countDown();
            gate.close();
        }
    }

    @Test
    void expiresQueuedTaskThroughPublicApi() throws Exception {

        FlowGate gate = FlowGate.builder()
                .globalConcurrency(1)
                .queue("default", q -> q.capacity(10))
                .build();

        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch releaseBlocker = new CountDownLatch(1);
        AtomicBoolean expiredTaskExecuted = new AtomicBoolean(false);

        try {
            gate.start();

            // Occupy the only worker.
            Submission<Void> blocker = gate.submit("default", () -> {
                blockerStarted.countDown();

                try {
                    releaseBlocker.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            assertTrue(blockerStarted.await(3, TimeUnit.SECONDS));
            assertTrue(blocker.isAccepted());

            // Submit a task with a short deadline.
            Submission<Void> deadlineTask = gate.submit(
                    "default",
                    () -> expiredTaskExecuted.set(true),
                    Duration.ofMillis(150)
            );

            assertTrue(deadlineTask.isAccepted());

            TaskHandle<Void> handle = deadlineTask.handle();

            // Wait for expiration without releasing the worker.
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);

            while (handle.status() != TaskStatus.EXPIRED
                    && System.nanoTime() < end) {
                Thread.sleep(10);
            }

            assertEquals(
                    TaskStatus.EXPIRED,
                    handle.status(),
                    "Queued task should expire before it executes"
            );

            releaseBlocker.countDown();

            blocker.handle().future().get(5, TimeUnit.SECONDS);

            assertFalse(
                    expiredTaskExecuted.get(),
                    "Expired task must not execute"
            );

        } finally {
            releaseBlocker.countDown();
            gate.close();
        }
    }
}