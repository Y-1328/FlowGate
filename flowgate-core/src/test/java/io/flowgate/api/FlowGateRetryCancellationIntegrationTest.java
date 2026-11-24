package io.flowgate.api;

import io.flowgate.policy.RetryPolicy;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class FlowGateRetryCancellationIntegrationTest {

    @Test
    void cancellationDuringRetryWaitPreventsNextAttempt() throws Exception {

        RetryPolicy retryPolicy = RetryPolicy.builder()
                .maxAttempts(3)
                .retryOn(IOException.class)
                .initialBackoff(Duration.ofSeconds(3))
                .multiplier(2.0)
                .maxBackoff(Duration.ofSeconds(5))
                .build();

        FlowGate gate = FlowGate.builder()
                .globalConcurrency(2)
                .queue("retry-cancel", q -> q
                        .capacity(10)
                        .retryPolicy(retryPolicy))
                .build();

        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch firstAttemptStarted = new CountDownLatch(1);

        try {
            gate.start();

            Submission<Void> submission = gate.submit(
                    "retry-cancel",
                    context -> {
                        attempts.incrementAndGet();
                        firstAttemptStarted.countDown();
                        throw new IOException("Temporary failure");
                    }
            );

            assertTrue(submission.isAccepted());

            TaskHandle handle = submission.handle();

            assertTrue(
                    firstAttemptStarted.await(5, TimeUnit.SECONDS),
                    "First attempt did not start in time"
            );

            // Wait until FlowGate has actually entered retry backoff.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);

            while (handle.status() != TaskStatus.RETRY_WAIT
                    && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }

            assertEquals(
                    TaskStatus.RETRY_WAIT,
                    handle.status(),
                    "Task should enter RETRY_WAIT before cancellation"
            );

            // Cancel the task while it is waiting for retry.
            assertTrue(handle.cancel(), "Cancellation should succeed");

            assertEquals(TaskStatus.CANCELLED, handle.status());

            // The task must not be executed again.
            assertEquals(1, attempts.get());

        } finally {
            gate.close();
        }
    }
}