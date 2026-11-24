package io.flowgate.internal.BT04;

import io.flowgate.internal.dispatch.DeficitRoundRobinScheduler;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertTrue;

class BT04_8RandomizedSchedulingAbuseTest {

    @Test
    void randomizedSchedulingPressureMaintainsValidDispatches() {

        var scheduler = new DeficitRoundRobinScheduler(2);
        var random = new Random(4048);

        int[] counts = new int[2];

        int[] quantum = {4, 1};

        for (int i = 0; i < 10000; i++) {

            boolean q0Eligible = random.nextBoolean();
            boolean q1Eligible = random.nextBoolean();

            int q0Waiting = random.nextInt(100);
            int q1Waiting = random.nextInt(100);

            boolean[] eligible = {
                    q0Eligible,
                    q1Eligible
            };

            int[] waiting = {
                    q0Waiting,
                    q1Waiting
            };

            int selected = scheduler.selectNext(
                    eligible,
                    waiting,
                    quantum
            );

            if (selected >= 0) {
                assertTrue(selected < 2,
                        "Scheduler returned invalid queue index: " + selected);

                counts[selected]++;
                scheduler.commitDispatch(selected);
            }
        }

        assertTrue(
                BT04TestSupport.total(counts) > 0,
                "Randomized workload produced no dispatches"
        );

        assertTrue(
                counts[0] >= 0 && counts[1] >= 0,
                "Dispatch counters became invalid"
        );
    }
}