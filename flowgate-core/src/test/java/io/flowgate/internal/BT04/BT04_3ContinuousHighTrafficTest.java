package io.flowgate.internal.BT04;

import io.flowgate.internal.dispatch.DeficitRoundRobinScheduler;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class BT04_3ContinuousHighTrafficTest {

    @Test
    void continuousHighTrafficDoesNotBlockOtherQueue() {

        var scheduler = new DeficitRoundRobinScheduler(2);

        int[] counts = BT04TestSupport.run(
                scheduler,
                10000,
                new boolean[]{true, true},
                new int[]{10000, 10000},
                new int[]{8, 1}
        );

        assertTrue(counts[0] > counts[1],
                "High-quantum queue should receive a larger share");

        assertTrue(counts[1] > 0,
                "Low-quantum queue should still receive dispatches");

        assertTrue(counts[1] >= 900,
                "Low-quantum queue progress unexpectedly low: " + counts[1]);
    }
}