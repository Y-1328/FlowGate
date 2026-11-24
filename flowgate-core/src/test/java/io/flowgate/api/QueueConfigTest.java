
package io.flowgate.api;

import io.flowgate.config.QueueConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class QueueConfigTest {

    @Test
    void shouldCreateValidQueueConfiguration() {
        QueueConfig config = new QueueConfig(
                "payments",
                Priority.CRITICAL,
                5,
                5000,
                50,
                70,
                Duration.ofSeconds(10)
        );

        assertEquals("payments", config.id());
        assertEquals(Priority.CRITICAL, config.priority());
        assertEquals(5, config.weight());
        assertEquals(5000, config.capacity());
        assertEquals(50, config.reservedConcurrency());
        assertEquals(70, config.maxConcurrency());
        assertEquals(Duration.ofSeconds(10), config.defaultDeadline());
    }

    @Test
    void shouldAllowMissingDefaultDeadline() {
        QueueConfig config = new QueueConfig(
                "notifications",
                Priority.NORMAL,
                2,
                1000,
                0,
                20,
                null
        );

        assertNull(config.defaultDeadline());
    }

    @Test
    void shouldRejectBlankQueueId() {
        assertThrows(IllegalArgumentException.class, () ->
                new QueueConfig(
                        " ",
                        Priority.NORMAL,
                        1, 100, 0, 10, null
                )
        );
    }

    @Test
    void shouldRejectInvalidWeight() {
        assertThrows(IllegalArgumentException.class, () ->
                new QueueConfig(
                        "payments",
                        Priority.HIGH,
                        0, 100, 0, 10, null
                )
        );
    }

    @Test
    void shouldRejectInvalidCapacity() {
        assertThrows(IllegalArgumentException.class, () ->
                new QueueConfig(
                        "payments",
                        Priority.HIGH,
                        1, 0, 0, 10, null
                )
        );
    }

    @Test
    void shouldRejectReservationAboveMaximum() {
        assertThrows(IllegalArgumentException.class, () ->
                new QueueConfig(
                        "payments",
                        Priority.CRITICAL,
                        5, 100, 20, 10, null
                )
        );
    }

    @Test
    void shouldRejectNonPositiveDeadline() {
        assertThrows(IllegalArgumentException.class, () ->
                new QueueConfig(
                        "payments",
                        Priority.CRITICAL,
                        5, 100, 0, 10, Duration.ZERO
                )
        );
    }

    @Test
    void shouldRejectNullPriority() {
        assertThrows(NullPointerException.class, () ->
                new QueueConfig(
                        "payments",
                        null,
                        1, 100, 0, 10, null
                )
        );
    }
}
