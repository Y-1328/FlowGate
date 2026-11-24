
package io.flowgate.policy;

import io.flowgate.api.Priority;
import io.flowgate.policy.PriorityAwareAgingPolicy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PriorityAwareAgingPolicyTest {

    private static final long SECOND = 1_000_000_000L;

    private PriorityAwareAgingPolicy policy;

    @BeforeEach
    void setUp() {
        policy = new PriorityAwareAgingPolicy(
                5 * SECOND,
                3,
                Map.of(
                        Priority.CRITICAL, 0,
                        Priority.HIGH, 1,
                        Priority.NORMAL, 3,
                        Priority.LOW, 6
                )
        );
    }

    @Test
    void shouldApplyPrioritySpecificMaximumBoosts() {
        assertEquals(0, policy.maxBoostFor(Priority.CRITICAL));
        assertEquals(1, policy.maxBoostFor(Priority.HIGH));
        assertEquals(3, policy.maxBoostFor(Priority.NORMAL));
        assertEquals(6, policy.maxBoostFor(Priority.LOW));
    }

    @Test
    void shouldNotAgeCriticalPriority() {
        assertEquals(
                8,
                policy.effectiveQuantum(
                        Priority.CRITICAL, 8, 100 * SECOND
                )
        );
    }

    @Test
    void shouldApplyBoundedAgingToHighPriority() {
        assertEquals(
                4,
                policy.effectiveQuantum(Priority.HIGH, 2, 5 * SECOND)
        );

        assertEquals(
                4,
                policy.effectiveQuantum(Priority.HIGH, 2, 100 * SECOND)
        );
    }

    @Test
    void shouldApplyBoundedAgingToNormalPriority() {
        assertEquals(
                4,
                policy.effectiveQuantum(Priority.NORMAL, 1, 15 * SECOND)
        );

        assertEquals(
                4,
                policy.effectiveQuantum(Priority.NORMAL, 1, 100 * SECOND)
        );
    }

    @Test
    void shouldAllowLowPriorityToEarnLargerBoost() {
        assertEquals(
                7,
                policy.effectiveQuantum(Priority.LOW, 1, 30 * SECOND)
        );

        assertEquals(
                7,
                policy.effectiveQuantum(Priority.LOW, 1, 100 * SECOND)
        );
    }

    @Test
    void shouldUseDefaultBoostForUnconfiguredPriority() {
        Priority custom = new Priority("CUSTOM", 50);

        assertEquals(3, policy.maxBoostFor(custom));

        assertEquals(
                8,
                policy.effectiveQuantum(custom, 2, 15 * SECOND)
        );
    }

    @Test
    void shouldUseBaseWeightBeforeAging() {
        assertEquals(
                8,
                policy.effectiveQuantum(Priority.CRITICAL, 8, 0)
        );

        assertEquals(
                4,
                policy.effectiveQuantum(Priority.HIGH, 4, 0)
        );

        assertEquals(
                2,
                policy.effectiveQuantum(Priority.NORMAL, 2, 0)
        );

        assertEquals(
                1,
                policy.effectiveQuantum(Priority.LOW, 1, 0)
        );
    }

    @Test
    void shouldRejectInvalidConfiguration() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new PriorityAwareAgingPolicy(0, 2, Map.of())
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> new PriorityAwareAgingPolicy(SECOND, -1, Map.of())
        );

        assertThrows(
                NullPointerException.class,
                () -> new PriorityAwareAgingPolicy(SECOND, 2, null)
        );
    }

    @Test
    void shouldRejectNegativePriorityBoost() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new PriorityAwareAgingPolicy(
                        SECOND,
                        2,
                        Map.of(Priority.LOW, -1)
                )
        );
    }

    @Test
    void shouldRejectInvalidInputs() {
        assertThrows(
                NullPointerException.class,
                () -> policy.effectiveQuantum(null, 1, 0)
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> policy.effectiveQuantum(Priority.NORMAL, 0, 0)
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> policy.effectiveQuantum(Priority.NORMAL, 1, -1)
        );
    }

    @Test
    void shouldExposeDefaultConfiguration() {
        assertEquals(3, policy.defaultMaxBoost());
        assertEquals(5 * SECOND, policy.agingIntervalNanos());
    }
}
