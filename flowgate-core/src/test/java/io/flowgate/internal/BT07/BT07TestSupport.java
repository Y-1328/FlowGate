package io.flowgate.internal.BT07;

import io.flowgate.api.Priority;
import io.flowgate.config.FlowGateConfig;
import io.flowgate.config.QueueConfig;
import io.flowgate.internal.runtime.FlowGateRuntime;
import io.flowgate.policy.RetryPolicy;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;

final class BT07TestSupport {

    private BT07TestSupport() {
    }

    static RetryPolicy retry(int maxAttempts, Duration backoff) {
        return RetryPolicy.builder()
                .maxAttempts(maxAttempts)
                .retryOn(IOException.class)
                .initialBackoff(backoff)
                .maxBackoff(backoff.multipliedBy(4))
                .multiplier(2.0)
                .build();
    }

    static QueueConfig queue(
            String id,
            int capacity,
            int maxConcurrency,
            RetryPolicy retryPolicy
    ) {
        return new QueueConfig(
                id,
                Priority.NORMAL,
                1,
                capacity,
                0,
                maxConcurrency,
                null,
                retryPolicy
        );
    }

    static FlowGateRuntime runtime(
            int globalConcurrency,
            QueueConfig... queues
    ) {
        return new FlowGateRuntime(
                new FlowGateConfig(globalConcurrency, List.of(queues)),
                Duration.ofMillis(2)
        );
    }

    static void await(
            AtomicInteger counter,
            int expected,
            Duration timeout,
            String message
    ) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();

        while (counter.get() < expected && System.nanoTime() < deadline) {
            TimeUnit.MILLISECONDS.sleep(2);
        }

        assertTrue(
                counter.get() >= expected,
                message + "; expected at least " + expected
                        + " but observed " + counter.get()
        );
    }

    static void awaitLatch(
            java.util.concurrent.CountDownLatch latch,
            Duration timeout,
            String message
    ) throws InterruptedException {
        assertTrue(
                latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS),
                message
        );
    }
}