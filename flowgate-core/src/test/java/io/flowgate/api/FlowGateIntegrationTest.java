package io.flowgate.api;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class FlowGateReuseIntegrationTest {

    @Test
    void singleFlowGateShouldAcceptMultipleSubmissionsAndBeReusable() throws Exception {

        AtomicInteger executionCount = new AtomicInteger(0);

        FlowGate gate = FlowGate.builder()
                .globalConcurrency(2)
                .queue("default", q -> q
                        .capacity(10))
                .build();

        try (gate) {

            // Start the SAME FlowGate once
            gate.start();

            // First batch
            Submission<Void> first = gate.submit(
                    "default",
                    executionCount::incrementAndGet
            );

            Submission<Void> second = gate.submit(
                    "default",
                    executionCount::incrementAndGet
            );

            assertTrue(first.isAccepted());
            assertTrue(second.isAccepted());

            // Each submission should have its own ID
            assertNotEquals(first.id(), second.id());

            // Wait for first batch to finish
            first.future().get();
            second.future().get();

            assertEquals(2, executionCount.get());

            // Submit again AFTER the previous submissions completed
            Submission<Void> third = gate.submit(
                    "default",
                    executionCount::incrementAndGet
            );

            Submission<Void> fourth = gate.submit(
                    "default",
                    executionCount::incrementAndGet
            );

            assertTrue(third.isAccepted());
            assertTrue(fourth.isAccepted());

            assertNotEquals(third.id(), fourth.id());
            assertNotEquals(first.id(), third.id());
            assertNotEquals(second.id(), fourth.id());

            third.future().get();
            fourth.future().get();

            // All four independent submissions executed
            assertEquals(4, executionCount.get());

            assertTrue(gate.isRunning());
        }
    }
}