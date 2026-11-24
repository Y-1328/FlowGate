package io.flowgate.api;

import io.flowgate.exception.RetryFailedException;
import io.flowgate.policy.RetryPolicy;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class FlowGateRetryExhaustionIntegrationTest {

    @Test
    void failsAfterMaximumRetryAttempts() throws Exception {

        RetryPolicy retryPolicy = RetryPolicy.builder()
                .maxAttempts(3)
                .retryOn(IOException.class)
                .initialBackoff(Duration.ofMillis(20))
                .multiplier(2.0)
                .maxBackoff(Duration.ofMillis(100))
                .build();

        FlowGate gate = FlowGate.builder()
                .globalConcurrency(2)
                .queue("retry-exhaustion", q -> q
                        .capacity(10)
                        .retryPolicy(retryPolicy))
                .build();

        AtomicInteger attempts = new AtomicInteger();

        try {
            gate.start();

            Submission<Void> submission = gate.submit(
                    "retry-exhaustion",
                    context -> {
                        attempts.incrementAndGet();
                        throw new IOException("Persistent failure");
                    }
            );

            assertTrue(submission.isAccepted());

            ExecutionException exception = assertThrows(
                    ExecutionException.class,
                    () -> submission.handle()
                            .future()
                            .get(5, TimeUnit.SECONDS)
            );

            Throwable cause = exception.getCause();

            assertInstanceOf(
                    RetryFailedException.class,
                    cause
            );

            assertEquals(3, attempts.get());

        } finally {
            gate.close();
        }
    }
}