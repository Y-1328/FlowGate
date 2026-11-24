
package io.flowgate.policy;

import io.flowgate.policy.PriorityAgingPolicy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PriorityAgingPolicyTest {

    private static final long SECOND = 1_000_000_000L;

    private PriorityAgingPolicy policy;

    @BeforeEach
    void setUp() {
        policy = new PriorityAgingPolicy(5 * SECOND, 4);
    }

    @Test
    void shouldStartWithBaseBoost() {
        assertEquals(1, policy.boost(0));
        assertEquals(1, policy.boost(4 * SECOND));
    }

    @Test
    void shouldIncreaseBoostAfterEachAgingInterval() {
        assertEquals(2, policy.boost(5 * SECOND));
        assertEquals(3, policy.boost(10 * SECOND));
        assertEquals(4, policy.boost(15 * SECOND));
        assertEquals(5, policy.boost(20 * SECOND));
    }

    @Test
    void shouldCapBoostAtConfiguredMaximum() {
        assertEquals(5, policy.boost(25 * SECOND));
        assertEquals(5, policy.boost(100 * SECOND));
        assertEquals(5, policy.boost(Long.MAX_VALUE));
    }

    @Test
    void shouldCalculateEffectiveQuantum() {
        assertEquals(5, policy.effectiveQuantum(5, 0));
        assertEquals(10, policy.effectiveQuantum(5, 5 * SECOND));
        assertEquals(15, policy.effectiveQuantum(5, 10 * SECOND));
        assertEquals(25, policy.effectiveQuantum(5, 20 * SECOND));
    }

    @Test
    void shouldPreserveRelativeWeightsWithoutAging() {
        assertEquals(5, policy.effectiveQuantum(5, 0));
        assertEquals(3, policy.effectiveQuantum(3, 0));
        assertEquals(2, policy.effectiveQuantum(2, 0));
    }

    @Test
    void shouldAllowZeroMaximumBoost() {
        PriorityAgingPolicy noAging =
                new PriorityAgingPolicy(5 * SECOND, 0);

        assertEquals(1, noAging.boost(0));
        assertEquals(1, noAging.boost(100 * SECOND));
        assertEquals(5, noAging.effectiveQuantum(5, 100 * SECOND));
    }

    @Test
    void shouldRejectNonPositiveAgingInterval() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new PriorityAgingPolicy(0, 4)
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> new PriorityAgingPolicy(-1, 4)
        );
    }

    @Test
    void shouldRejectNegativeMaximumBoost() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new PriorityAgingPolicy(5 * SECOND, -1)
        );
    }

    @Test
    void shouldRejectNegativeHeadWait() {
        assertThrows(
                IllegalArgumentException.class,
                () -> policy.boost(-1)
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> policy.effectiveQuantum(5, -1)
        );
    }

    @Test
    void shouldRejectNonPositiveBaseWeight() {
        assertThrows(
                IllegalArgumentException.class,
                () -> policy.effectiveQuantum(0, 0)
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> policy.effectiveQuantum(-1, 0)
        );
    }

    @Test
    void shouldCapEffectiveQuantumAtIntegerMaximum() {
        PriorityAgingPolicy largeBoost =
                new PriorityAgingPolicy(1, Integer.MAX_VALUE - 1);

        assertEquals(
                Integer.MAX_VALUE,
                largeBoost.effectiveQuantum(
                        Integer.MAX_VALUE,
                        Long.MAX_VALUE
                )
        );
    }

    @Test
    void shouldExposeConfiguration() {
        assertEquals(5 * SECOND, policy.agingIntervalNanos());
        assertEquals(4, policy.maxBoost());
    }
}
