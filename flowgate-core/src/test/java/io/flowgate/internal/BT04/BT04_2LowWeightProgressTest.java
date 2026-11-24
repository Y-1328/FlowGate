package io.flowgate.internal.BT04;

import io.flowgate.internal.dispatch.DeficitRoundRobinScheduler;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class BT04_2LowWeightProgressTest {

    @Test
    void lowWeightQueueDoesNotStarve() {

        var scheduler = new DeficitRoundRobinScheduler(2);

        int[] counts = BT04TestSupport.run(
                scheduler,
                4000,
                new boolean[]{true, true},
                new int[]{1000, 1000},
                new int[]{4, 1}
        );

        assertTrue(
                counts[1] > 0,
                "Low-weight queue received no dispatches"
        );

        assertTrue(
                counts[1] >= 500,
                "Low-weight queue progress is below expected minimum: " + counts[1]
        );
    }
}