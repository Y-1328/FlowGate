
package io.flowgate.internal.dispatch;

import io.flowgate.api.Priority;
import io.flowgate.policy.PriorityAwareAgingPolicy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class StarvationBoundTest {

    private static final long SECOND = 1_000_000_000L;
    private static final int DECISIONS = 100_000;

    private DeficitRoundRobinScheduler scheduler;
    private PriorityAwareAgingPolicy agingPolicy;

    private static final Priority[] PRIORITIES = {
            Priority.CRITICAL,
            Priority.LOW
    };

    private static final int[] WEIGHTS = {8, 1};

    @BeforeEach
    void setUp() {
        scheduler = new DeficitRoundRobinScheduler(2);

        agingPolicy = new PriorityAwareAgingPolicy(
                5 * SECOND,
                3,
                Map.of(
                        Priority.CRITICAL, 0,
                        Priority.LOW, 6
                )
        );
    }

    @Test
    void shouldGiveLowPriorityMeaningfulSchedulingShare() {

        int[] quantum = {
                agingPolicy.effectiveQuantum(
                        PRIORITIES[0], WEIGHTS[0], 0
                ),
                agingPolicy.effectiveQuantum(
                        PRIORITIES[1], WEIGHTS[1], 30 * SECOND
                )
        };

        boolean[] eligible = {true, true};
        int[] waiting = {10_000, 10_000};

        int[] selections = new int[2];

        for (int i = 0; i < DECISIONS; i++) {

            int selected = scheduler.selectNext(
                    eligible,
                    waiting,
                    quantum
            );

            assertTrue(selected >= 0);

            selections[selected]++;

            // Each decision represents a successful dispatch.
            scheduler.commitDispatch(selected);
        }

        double lowShare =
                (double) selections[1] / DECISIONS;

        assertTrue(
                lowShare >= 0.40,
                "LOW should receive at least 40% scheduling share"
        );

        assertEquals(
                DECISIONS,
                selections[0] + selections[1]
        );
    }

    @Test
    void shouldBoundConsecutiveCriticalSelections() {

        int[] quantum = {
                agingPolicy.effectiveQuantum(
                        PRIORITIES[0], WEIGHTS[0], 0
                ),
                agingPolicy.effectiveQuantum(
                        PRIORITIES[1], WEIGHTS[1], 30 * SECOND
                )
        };

        boolean[] eligible = {true, true};
        int[] waiting = {10_000, 10_000};

        int consecutiveCritical = 0;
        int maximumCriticalGap = 0;

        for (int i = 0; i < DECISIONS; i++) {

            int selected = scheduler.selectNext(
                    eligible,
                    waiting,
                    quantum
            );

            assertTrue(selected >= 0);

            if (selected == 0) {
                consecutiveCritical++;

                maximumCriticalGap = Math.max(
                        maximumCriticalGap,
                        consecutiveCritical
                );
            } else {
                consecutiveCritical = 0;
            }

            // Both CRITICAL and LOW selections represent successful dispatches.
            scheduler.commitDispatch(selected);
        }

        assertTrue(
                maximumCriticalGap <= 8,
                "LOW should not be skipped for more than 8 consecutive decisions"
        );
    }
}
