package io.flowgate.internal.BT04;

import io.flowgate.internal.dispatch.DeficitRoundRobinScheduler;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class BT04_4QueueMonopolizationTest {

    @Test
    void monopolizingQueueCannotPreventNewQueueFromRunning() {

        var scheduler = new DeficitRoundRobinScheduler(2);

        // Phase 1: queue 0 monopolizes eligibility.
        int[] phase1 = BT04TestSupport.run(
                scheduler,
                2000,
                new boolean[]{true, false},
                new int[]{2000, 0},
                new int[]{1, 1}
        );

        assertTrue(phase1[0] > 0,
                "Eligible queue should have received dispatches");

        // Phase 2: queue 1 becomes eligible.
        int[] phase2 = BT04TestSupport.run(
                scheduler,
                2000,
                new boolean[]{true, true},
                new int[]{2000, 2000},
                new int[]{1, 1}
        );

        assertTrue(phase2[1] > 0,
                "Newly eligible queue was starved");

        assertTrue(phase2[0] > 0,
                "Original queue should also continue receiving dispatches");
    }
}