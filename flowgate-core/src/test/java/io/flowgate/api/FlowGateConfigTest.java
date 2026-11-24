package io.flowgate.api;

import io.flowgate.config.FlowGateConfig;
import io.flowgate.config.QueueConfig;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FlowGateConfigTest {

    private QueueConfig payments() {
        return new QueueConfig(
                "payments",
                Priority.CRITICAL,
                5,
                5000,
                50,
                70,
                null
        );
    }

    private QueueConfig notifications() {
        return new QueueConfig(
                "notifications",
                Priority.NORMAL,
                2,
                3000,
                10,
                30,
                null
        );
    }

    @Test
    void shouldCreateValidConfiguration() {
        FlowGateConfig config = new FlowGateConfig(
                100,
                List.of(payments(), notifications())
        );

        assertEquals(100, config.globalConcurrency());
        assertEquals(2, config.queues().size());
    }

    @Test
    void shouldRejectNonPositiveGlobalConcurrency() {
        assertThrows(IllegalArgumentException.class, () ->
                new FlowGateConfig(0, List.of(payments()))
        );
    }

    @Test
    void shouldRejectNegativeGlobalConcurrency() {
        assertThrows(IllegalArgumentException.class, () ->
                new FlowGateConfig(-1, List.of(payments()))
        );
    }

    @Test
    void shouldRejectNullQueueList() {
        assertThrows(NullPointerException.class, () ->
                new FlowGateConfig(100, null)
        );
    }

    @Test
    void shouldRejectEmptyQueueList() {
        assertThrows(IllegalArgumentException.class, () ->
                new FlowGateConfig(100, List.of())
        );
    }

    @Test
    void shouldRejectNullQueueElement() {
        List<QueueConfig> queues = new ArrayList<>();
        queues.add(payments());
        queues.add(null);

        assertThrows(NullPointerException.class, () ->
                new FlowGateConfig(100, queues)
        );
    }

    @Test
    void shouldDefensivelyCopyQueueList() {
        List<QueueConfig> queues = new ArrayList<>();
        queues.add(payments());

        FlowGateConfig config = new FlowGateConfig(100, queues);

        queues.add(notifications());

        assertEquals(1, config.queues().size());
    }

    @Test
    void shouldExposeUnmodifiableQueueList() {
        FlowGateConfig config = new FlowGateConfig(
                100,
                List.of(payments())
        );

        assertThrows(UnsupportedOperationException.class, () ->
                config.queues().add(notifications())
        );
    }
}