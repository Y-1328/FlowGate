package io.flowgate.internal.BT04;

import io.flowgate.internal.dispatch.DeficitRoundRobinScheduler;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class BT04_6DynamicEligibilityTest {

    @Test
    void schedulerHandlesRepeatedEligibilityChanges() {

        var scheduler = new DeficitRoundRobinScheduler(2);

        int queue0Dispatches = 0;
        int queue1Dispatches = 0;

        for (int cycle = 0; cycle < 100; cycle++) {

            // Queue 0 only.
            for (int i = 0; i < 20; i++) {
                int selected = scheduler.selectNext(
                        new boolean[]{true, false},
                        new int[]{20, 0},
                        new int[]{2, 1}
                );

                if (selected >= 0) {
                    if (selected == 0) queue0Dispatches++;
                    scheduler.commitDispatch(selected);
                }
            }

            // Both queues become eligible.
            for (int i = 0; i < 20; i++) {
                int selected = scheduler.selectNext(
                        new boolean[]{true, true},
                        new int[]{20, 20},
                        new int[]{2, 1}
                );

                if (selected >= 0) {
                    if (selected == 0) queue0Dispatches++;
                    if (selected == 1) queue1Dispatches++;
                    scheduler.commitDispatch(selected);
                }
            }

            // Queue 1 disappears.
            for (int i = 0; i < 20; i++) {
                int selected = scheduler.selectNext(
                        new boolean[]{true, false},
                        new int[]{20, 0},
                        new int[]{2, 1}
                );

                if (selected >= 0) {
                    if (selected == 0) queue0Dispatches++;
                    scheduler.commitDispatch(selected);
                }
            }
        }

        assertTrue(queue0Dispatches > 0,
                "Queue 0 received no dispatches");

        assertTrue(queue1Dispatches > 0,
                "Queue 1 failed to progress when eligible");
    }
}