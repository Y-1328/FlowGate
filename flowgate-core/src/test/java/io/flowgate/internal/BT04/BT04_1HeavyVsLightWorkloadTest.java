package io.flowgate.internal.BT04;

import io.flowgate.internal.dispatch.DeficitRoundRobinScheduler;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class BT04_1HeavyVsLightWorkloadTest {

    @Test
    void heavyWorkloadReceivesProportionalSchedulingShare() {

        var scheduler = new DeficitRoundRobinScheduler(2);

        int[] counts = BT04TestSupport.run(
                scheduler,
                4000,
                new boolean[]{true, true},
                new int[]{1000, 1000},
                new int[]{4, 1}
        );

        double heavyRatio = BT04TestSupport.ratio(counts, 0);
        double lightRatio = BT04TestSupport.ratio(counts, 1);

        assertTrue(
                heavyRatio >= 0.75 && heavyRatio <= 0.85,
                "Expected heavy queue to receive approximately 80% share. Actual: "
                        + counts[0] + ":" + counts[1]
        );

        assertTrue(
                lightRatio >= 0.15 && lightRatio <= 0.25,
                "Expected light queue to receive approximately 20% share. Actual: "
                        + counts[0] + ":" + counts[1]
        );
    }
}