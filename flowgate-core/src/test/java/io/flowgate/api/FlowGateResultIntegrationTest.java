package io.flowgate.api;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class FlowGateResultIntegrationTest {

    @Test
    void retrievesTaskResultThroughPublicApi() throws Exception {

        FlowGate gate = FlowGate.builder()
                .globalConcurrency(2)
                .queue("default", q -> q.capacity(10))
                .build();

        try {
            gate.start();

            Submission<Integer> submission =
                    gate.submit("default", context -> 42);

            assertTrue(submission.isAccepted());

            Integer result = submission.handle()
                    .future()
                    .get(5, TimeUnit.SECONDS);

            assertEquals(42, result);

        } finally {
            gate.close();
        }
    }

    @Test
    void propagatesTaskFailureThroughPublicApi() throws Exception {

        FlowGate gate = FlowGate.builder()
                .globalConcurrency(2)
                .queue("default", q -> q.capacity(10))
                .build();

        RuntimeException expected =
                new RuntimeException("Simulated task failure");

        try {
            gate.start();

            Submission<Void> submission = gate.submit(
                    "default",
                    () -> {
                        throw expected;
                    }
            );

            assertTrue(submission.isAccepted());

            ExecutionException thrown = assertThrows(
                    ExecutionException.class,
                    () -> submission.handle()
                            .future()
                            .get(5, TimeUnit.SECONDS)
            );

            assertSame(expected, thrown.getCause());

        } finally {
            gate.close();
        }
    }
}