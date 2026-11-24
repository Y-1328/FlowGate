package io.flowgate.internal.BT04;

import io.flowgate.internal.dispatch.DeficitRoundRobinScheduler;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class BT04_5BurstRecoveryTest {

    @Test
    void schedulerRecoversAfterBurstTraffic() {

        var scheduler = new DeficitRoundRobinScheduler(2);

        // Normal phase: queue 0 only.
        int[] normalBefore = BT04TestSupport.run(
                scheduler,
                1000,
                new boolean[]{true, false},
                new int[]{1000, 0},
                new int[]{3, 1}
        );

        assertTrue(normalBefore[0] > 0);

        // Burst phase: both queues suddenly become active.
        int[] burst = BT04TestSupport.run(
                scheduler,
                5000,
                new boolean[]{true, true},
                new int[]{5000, 5000},
                new int[]{3, 1}
        );

        assertTrue(burst[0] > 0);
        assertTrue(burst[1] > 0);

        // Recovery phase: queue 1 becomes inactive again.
        int[] recovery = BT04TestSupport.run(
                scheduler,
                1000,
                new boolean[]{true, false},
                new int[]{1000, 0},
                new int[]{3, 1}
        );

        assertTrue(
                recovery[0] == 1000,
                "Scheduler did not return to single-queue dispatch behavior"
        );
    }
}