package io.flowgate.internal.dispatch;

import io.flowgate.api.Priority;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import io.flowgate.policy.PriorityAgingPolicy;

import java.util.Map;
import io.flowgate.policy.PriorityAwareAgingPolicy;

import static org.junit.jupiter.api.Assertions.*;

class SchedulerAgingIntegrationTest {

    private static final long SECOND = 1_000_000_000L;

    private DeficitRoundRobinScheduler scheduler;
    private io.flowgate.policy.PriorityAwareAgingPolicy agingPolicy;

    private static final Priority[] PRIORITIES = {
            Priority.CRITICAL,
            Priority.LOW
    };

    private static final int[] WEIGHTS = {8, 1};

    @BeforeEach
    void setUp() {
        scheduler = new DeficitRoundRobinScheduler(2);

        agingPolicy = new io.flowgate.policy.PriorityAwareAgingPolicy(
                5 * SECOND,
                3,
                Map.of(
                        Priority.CRITICAL, 0,
                        Priority.LOW, 6
                )
        );
    }

    private int[] calculateQuanta(long[] headWaitNanos) {
        int[] quantum = new int[PRIORITIES.length];

        for (int i = 0; i < PRIORITIES.length; i++) {
            quantum[i] = agingPolicy.effectiveQuantum(
                    PRIORITIES[i],
                    WEIGHTS[i],
                    headWaitNanos[i]
            );
        }

        return quantum;
    }

    private int[] simulate(
            int decisions,
            boolean[] eligible,
            int[] waiting,
            long[] headWaitNanos
    ) {
        int[] quantum = calculateQuanta(headWaitNanos);
        int[] selections = new int[PRIORITIES.length];

        for (int i = 0; i < decisions; i++) {
            int selected = scheduler.selectNext(eligible, waiting, quantum);

            if (selected >= 0) {
                selections[selected]++;

                // A simulation decision represents a successful dispatch.
                scheduler.commitDispatch(selected);
            }
        }

        return selections;
    }

    @Test
    void shouldPreserveBaseWeightedSchedulingWithoutAging() {
        int[] selections = simulate(
                15_000,
                new boolean[]{true, true},
                new int[]{10_000, 10_000},
                new long[]{0, 0}
        );

        assertEquals(13_334, selections[0]);
        assertEquals(1_666, selections[1]);
    }

    @Test
    void shouldIncreaseLowPrioritySchedulingShareThroughAging() {
        int[] baseline = simulate(
                15_000,
                new boolean[]{true, true},
                new int[]{10_000, 10_000},
                new long[]{0, 0}
        );

        scheduler.reset();

        int[] aged = simulate(
                15_000,
                new boolean[]{true, true},
                new int[]{10_000, 10_000},
                new long[]{0, 30 * SECOND}
        );

        assertTrue(
                aged[1] > baseline[1],
                "Aging should increase LOW priority's scheduling share"
        );
    }

    @Test
    void shouldRespectPrioritySpecificAgingCeilings() {
        int[] quantum = calculateQuanta(
                new long[]{100 * SECOND, 100 * SECOND}
        );

        assertEquals(8, quantum[0]);
        assertEquals(7, quantum[1]);
    }

    @Test
    void shouldPreventLowPriorityStarvationUnderSustainedLoad() {
        int[] selections = simulate(
                15_000,
                new boolean[]{true, true},
                new int[]{10_000, 10_000},
                new long[]{0, 30 * SECOND}
        );

        assertTrue(
                selections[1] > 0,
                "LOW priority must receive scheduling opportunities"
        );

        assertTrue(
                selections[0] > 0,
                "CRITICAL priority must continue receiving scheduling opportunities"
        );
    }

    @Test
    void shouldNeverSelectIneligibleQueue() {
        boolean[] eligible = {true, false};
        int[] waiting = {100, 100};
        int[] quantum = calculateQuanta(
                new long[]{0, 30 * SECOND}
        );

        for (int i = 0; i < 100; i++) {
            int selected = scheduler.selectNext(
                    eligible,
                    waiting,
                    quantum
            );

            assertEquals(0, selected);

            // The selected eligible queue represents a successful dispatch.
            scheduler.commitDispatch(selected);
        }
    }

    @Test
    void shouldReturnNoSelectionWhenAllQueuesAreIneligible() {
        int selected = scheduler.selectNext(
                new boolean[]{false, false},
                new int[]{100, 100},
                new int[]{8, 7}
        );

        assertEquals(-1, selected);
    }

    @Test
    void shouldReturnNoSelectionWhenQueuesAreEmpty() {
        int selected = scheduler.selectNext(
                new boolean[]{true, true},
                new int[]{0, 0},
                new int[]{8, 7}
        );

        assertEquals(-1, selected);
    }

    @Test
    void shouldRecalculateQuantumWhenWaitingTimeChanges() {
        int[] initial = calculateQuanta(
                new long[]{0, 0}
        );

        int[] aged = calculateQuanta(
                new long[]{0, 30 * SECOND}
        );

        assertEquals(1, initial[1]);
        assertEquals(7, aged[1]);

        assertEquals(8, initial[0]);
        assertEquals(8, aged[0]);
    }
}