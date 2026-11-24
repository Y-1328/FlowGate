package io.flowgate.internal.BT04;

import io.flowgate.internal.dispatch.DeficitRoundRobinScheduler;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class BT04_7LongRunFairnessDriftTest {

    @Test
    void longRunWeightedFairnessDoesNotDrift() {

        var scheduler = new DeficitRoundRobinScheduler(2);

        int[] counts = BT04TestSupport.run(
                scheduler,
                100000,
                new boolean[]{true, true},
                new int[]{100000, 100000},
                new int[]{4, 1}
        );

        double heavyRatio = BT04TestSupport.ratio(counts, 0);
        double lightRatio = BT04TestSupport.ratio(counts, 1);

        assertTrue(
                heavyRatio >= 0.78 && heavyRatio <= 0.82,
                "Long-run fairness drift detected. Heavy ratio: " + heavyRatio
        );

        assertTrue(
                lightRatio >= 0.18 && lightRatio <= 0.22,
                "Long-run fairness drift detected. Light ratio: " + lightRatio
        );
    }
}