package io.flowgate.api;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class FlowGatePublicApiTest {

    private FlowGate createGate() {
        return FlowGate.builder()
                .globalConcurrency(4)
                .queue("test", q -> q
                        .priority(Priority.NORMAL)
                        .weight(1)
                        .capacity(10)
                        .maxConcurrency(4))
                .build();
    }

    @Test
    void shouldBuildFlowGate() {
        FlowGate gate = createGate();

        assertNotNull(gate);
        gate.close();
    }

    @Test
    void shouldStartFlowGate() {
        FlowGate gate = createGate();

        gate.start();

        assertTrue(gate.isRunning());

        gate.close();
    }

    @Test
    void shouldExecuteRunnableTask() throws Exception {
        try (FlowGate gate = createGate()) {
            gate.start();

            Submission<Void> submission =
                    gate.submit("test", () -> System.out.println("Task executed"));

            assertTrue(submission.isAccepted());

            submission.handle()
                    .future()
                    .get(3, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldExecuteContextAwareTask() throws Exception {
        try (FlowGate gate = createGate()) {
            gate.start();

            Submission<String> submission =
                    gate.submit("test", context -> "FlowGate works");

            assertTrue(submission.isAccepted());

            String result = submission.handle()
                    .future()
                    .get(3, TimeUnit.SECONDS);

            assertEquals("FlowGate works", result);
        }
    }

    @Test
    void shouldCompleteTaskSuccessfully() throws Exception {
        try (FlowGate gate = createGate()) {
            gate.start();

            Submission<Integer> submission =
                    gate.submit("test", context -> 42);

            Integer result = submission.handle()
                    .future()
                    .get(3, TimeUnit.SECONDS);

            assertEquals(42, result);
        }
    }

    @Test
    void shouldAcceptTaskWithDeadline() {
        try (FlowGate gate = createGate()) {
            gate.start();

            Submission<Void> submission =
                    gate.submit(
                            "test",
                            () -> {},
                            java.time.Duration.ofSeconds(5)
                    );

            assertTrue(submission.isAccepted());
        }
    }

    @Test
    void shouldRejectInvalidConcurrency() {
        assertThrows(IllegalArgumentException.class, () ->
                FlowGate.builder()
                        .globalConcurrency(0)
                        .queue("test", q -> q.capacity(10))
                        .build()
        );
    }

    @Test
    void shouldRequireAtLeastOneQueue() {
        assertThrows(IllegalArgumentException.class, () ->
                FlowGate.builder()
                        .globalConcurrency(4)
                        .build()
        );
    }

    @Test
    void shouldRejectDuplicateQueueIds() {
        assertThrows(IllegalArgumentException.class, () ->
                FlowGate.builder()
                        .globalConcurrency(4)
                        .queue("test", q -> q.capacity(10))
                        .queue("test", q -> q.capacity(20))
                        .build()
        );
    }
}