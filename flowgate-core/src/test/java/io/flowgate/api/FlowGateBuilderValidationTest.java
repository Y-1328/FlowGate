package io.flowgate.api;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FlowGateBuilderValidationTest {

    @Test
    void rejectsQueueMaxConcurrencyGreaterThanGlobal() {
        assertThrows(IllegalArgumentException.class, () ->
                FlowGate.builder()
                        .globalConcurrency(4)
                        .queue("payments", q -> q
                                .capacity(100)
                                .maxConcurrency(5))
                        .build()
        );
    }

    @Test
    void rejectsReservedConcurrencyGreaterThanGlobal() {
        assertThrows(IllegalArgumentException.class, () ->
                FlowGate.builder()
                        .globalConcurrency(4)
                        .queue("payments", q -> q
                                .reservedConcurrency(5))
                        .build()
        );
    }

    @Test
    void rejectsTotalReservationsGreaterThanGlobal() {
        assertThrows(IllegalArgumentException.class, () ->
                FlowGate.builder()
                        .globalConcurrency(4)
                        .queue("payments", q -> q
                                .reservedConcurrency(3))
                        .queue("notifications", q -> q
                                .reservedConcurrency(2))
                        .build()
        );
    }

    @Test
    void rejectsInvalidQueueId() {
        assertThrows(IllegalArgumentException.class, () ->
                FlowGate.builder()
                        .globalConcurrency(4)
                        .queue("invalid queue name", q -> q.capacity(10))
                        .build()
        );
    }

    @Test
    void rejectsZeroQueueCapacity() {
        assertThrows(IllegalArgumentException.class, () ->
                FlowGate.builder()
                        .globalConcurrency(4)
                        .queue("payments", q -> q.capacity(0))
                        .build()
        );
    }

    @Test
    void rejectsZeroQueueWeight() {
        assertThrows(IllegalArgumentException.class, () ->
                FlowGate.builder()
                        .globalConcurrency(4)
                        .queue("payments", q -> q.weight(0))
                        .build()
        );
    }

    @Test
    void rejectsZeroMaxConcurrency() {
        assertThrows(IllegalArgumentException.class, () ->
                FlowGate.builder()
                        .globalConcurrency(4)
                        .queue("payments", q -> q.maxConcurrency(0))
                        .build()
        );
    }

    @Test
    void acceptsValidConfiguration() {
        assertDoesNotThrow(() ->
                FlowGate.builder()
                        .globalConcurrency(8)
                        .queue("payments", q -> q
                                .priority(Priority.CRITICAL)
                                .weight(5)
                                .capacity(100)
                                .reservedConcurrency(2)
                                .maxConcurrency(4))
                        .queue("notifications", q -> q
                                .priority(Priority.NORMAL)
                                .weight(1)
                                .capacity(500))
                        .build()
        );
    }
}