package io.flowgate.examples;

import io.flowgate.api.FlowGate;
import io.flowgate.api.Priority;
import io.flowgate.api.Submission;
import io.flowgate.policy.RetryPolicy;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

public class RetryExample {

    public static void main(String[] args) throws Exception {

        AtomicInteger attempts = new AtomicInteger();

        RetryPolicy retryPolicy = RetryPolicy.builder()
                .maxAttempts(3)
                .retryOn(IllegalStateException.class)
                .initialBackoff(Duration.ofMillis(100))
                .maxBackoff(Duration.ofSeconds(2))
                .multiplier(2.0)
                .build();

        FlowGate gate = FlowGate.builder()
                .globalConcurrency(2)
                .queue("retry-workload", q -> q
                        .priority(Priority.NORMAL)
                        .capacity(10)
                        .retryPolicy(retryPolicy))
                .build();

        try (gate) {

            gate.start();

            Submission<Void> submission = gate.submit(
                    "retry-workload",
                    () -> {

                        int attempt = attempts.incrementAndGet();

                        System.out.println("Executing attempt " + attempt);

                        if (attempt < 3) {
                            throw new IllegalStateException(
                                    "Simulated temporary failure");
                        }

                        System.out.println("Task succeeded on attempt " + attempt);
                    }
            );

            if (submission.isAccepted()) {

                submission.future().get();

                System.out.println("Final status: " + submission.status());
                System.out.println("Total attempts: " + attempts.get());

            } else {
                System.out.println("Rejected: " + submission.rejectionReason());
            }
        }
    }
}