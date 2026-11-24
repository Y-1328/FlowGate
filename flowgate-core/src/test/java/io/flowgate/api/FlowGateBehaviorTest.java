package io.flowgate.api;

import org.junit.jupiter.api.Test;

import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class FlowGateBehaviorTest {

    private FlowGate createGate(int capacity) {
        return FlowGate.builder()
                .globalConcurrency(1)
                .queue("test", q -> q
                        .priority(Priority.NORMAL)
                        .weight(1)
                        .capacity(capacity)
                        .maxConcurrency(1))
                .build();
    }

    @Test
    void shouldRejectWhenQueueIsFull() throws Exception {
        try (FlowGate gate = createGate(1)) {
            gate.start();

            CountDownLatch taskStarted = new CountDownLatch(1);
            CountDownLatch releaseTask = new CountDownLatch(1);

            // Occupy the only execution slot.
            Submission<Void> first = gate.submit("test", () -> {
                taskStarted.countDown();

                try {
                    releaseTask.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            assertTrue(taskStarted.await(3, TimeUnit.SECONDS));

            // Fill the queue.
            Submission<Void> second =
                    gate.submit("test", () -> {});

            assertTrue(second.isAccepted());

            // Queue is now full. This task should be rejected.
            Submission<Void> third =
                    gate.submit("test", () -> {});

            assertFalse(third.isAccepted());

            releaseTask.countDown();

            first.handle().future().get(3, TimeUnit.SECONDS);
            second.handle().future().get(3, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldPropagateTaskFailure() throws Exception {
        try (FlowGate gate = createGate(10)) {
            gate.start();

            Submission<Void> submission = gate.submit("test", () -> {
                throw new RuntimeException("Expected test failure");
            });

            assertTrue(submission.isAccepted());

            ExecutionException exception = assertThrows(
                    ExecutionException.class,
                    () -> submission.handle()
                            .future()
                            .get(3, TimeUnit.SECONDS)
            );

            assertEquals(
                    "Expected test failure",
                    exception.getCause().getMessage()
            );
        }
    }

    @Test
    void shouldAllowCancellationRequest() throws Exception {
        try (FlowGate gate = createGate(10)) {
            gate.start();

            CountDownLatch taskStarted = new CountDownLatch(1);
            CountDownLatch releaseTask = new CountDownLatch(1);

            // Block the worker.
            Submission<Void> first = gate.submit("test", () -> {
                taskStarted.countDown();

                try {
                    releaseTask.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            assertTrue(taskStarted.await(3, TimeUnit.SECONDS));

            // Submit a task that should remain queued.
            Submission<Void> queued = gate.submit("test", () -> {});

            assertTrue(queued.isAccepted());

            // Request cancellation of queued task.
            queued.handle().cancel();

            releaseTask.countDown();

            first.handle().future().get(3, TimeUnit.SECONDS);
        }
    }
}