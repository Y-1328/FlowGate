package io.flowgate.api;

import io.flowgate.policy.RetryPolicy;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class FlowGateRetryIntegrationTest {

    @Test
    void retriesFailedTaskAndSucceedsThroughPublicApi() throws Exception {

        RetryPolicy retryPolicy = RetryPolicy.builder()
                .maxAttempts(3)
                .retryOn(IOException.class)
                .initialBackoff(Duration.ofMillis(50))
                .multiplier(2.0)
                .maxBackoff(Duration.ofMillis(200))
                .build();

        FlowGate gate = FlowGate.builder()
                .globalConcurrency(2)
                .queue("retry-queue", q -> q
                        .capacity(10)
                        .retryPolicy(retryPolicy))
                .build();

        AtomicInteger attempts = new AtomicInteger();

        try {
            gate.start();

            Submission<String> submission = gate.submit(
                    "retry-queue",
                    context -> {
                        int attempt = attempts.incrementAndGet();

                        if (attempt < 3) {
                            throw new IOException(
                                    "Temporary failure on attempt " + attempt
                            );
                        }

                        return "success";
                    }
            );

            assertTrue(submission.isAccepted());

            String result = submission.handle()
                    .future()
                    .get(5, TimeUnit.SECONDS);

            assertEquals("success", result);
            assertEquals(3, attempts.get());

        } finally {
            gate.close();
        }
    }
}