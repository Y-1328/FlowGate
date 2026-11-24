
package io.flowgate.internal.dispatch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DeficitRoundRobinSchedulerTest {

    private DeficitRoundRobinScheduler scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new DeficitRoundRobinScheduler(3);
    }

    @Test
    void shouldRespectWeightedDispatchSequence() {
        boolean[] eligible = {true, true, true};
        int[] waiting = {100, 100, 100};
        int[] quantum = {5, 3, 2};

        int[] expected = {
                0, 0, 0, 0, 0,
                1, 1, 1,
                2, 2
        };

        for (int expectedQueue : expected) {
            int selected = scheduler.selectNext(
                    eligible, waiting, quantum
            );

            assertEquals(expectedQueue, selected);

            scheduler.commitDispatch(selected);
        }
    }

    @Test
    void shouldMaintainWeightedShareOverManySelections() {
        boolean[] eligible = {true, true, true};
        int[] waiting = {100, 100, 100};
        int[] quantum = {5, 3, 2};

        int[] counts = new int[3];

        for (int i = 0; i < 10_000; i++) {
            int selected = scheduler.selectNext(
                    eligible, waiting, quantum
            );

            assertTrue(selected >= 0 && selected < 3);

            counts[selected]++;

            scheduler.commitDispatch(selected);
        }

        assertEquals(5000, counts[0]);
        assertEquals(3000, counts[1]);
        assertEquals(2000, counts[2]);
    }

    @Test
    void shouldSkipIneligibleQueues() {
        boolean[] eligible = {false, true, true};
        int[] waiting = {100, 100, 100};
        int[] quantum = {5, 3, 2};

        for (int i = 0; i < 3; i++) {
            int selected = scheduler.selectNext(
                    eligible, waiting, quantum
            );

            assertEquals(1, selected);

            scheduler.commitDispatch(selected);
        }

        int selected = scheduler.selectNext(
                eligible, waiting, quantum
        );

        assertEquals(2, selected);

        scheduler.commitDispatch(selected);
    }

    @Test
    void shouldRetainDeficitWhenQueueTemporarilyBecomesIneligible() {
        boolean[] eligible = {true, true, true};
        int[] waiting = {100, 100, 100};
        int[] quantum = {5, 3, 2};

        int selected = scheduler.selectNext(
                eligible, waiting, quantum
        );

        assertEquals(0, selected);

        scheduler.commitDispatch(selected);

        assertEquals(4, scheduler.deficit(0));

        eligible[0] = false;

        selected = scheduler.selectNext(
                eligible, waiting, quantum
        );

        assertEquals(1, selected);

        scheduler.cancelSelection(selected);

        assertEquals(4, scheduler.deficit(0));
    }

    @Test
    void shouldResetDeficitWhenQueueBecomesEmpty() {
        boolean[] eligible = {true, true, true};
        int[] waiting = {100, 100, 100};
        int[] quantum = {5, 3, 2};

        int selected = scheduler.selectNext(
                eligible, waiting, quantum
        );

        scheduler.commitDispatch(selected);

        assertEquals(4, scheduler.deficit(0));

        waiting[0] = 0;

        selected = scheduler.selectNext(
                eligible, waiting, quantum
        );

        assertEquals(1, selected);

        scheduler.cancelSelection(selected);

        assertEquals(0, scheduler.deficit(0));
    }

    @Test
    void shouldReturnMinusOneWhenNoQueueIsEligible() {
        boolean[] eligible = {false, false, false};
        int[] waiting = {100, 100, 100};
        int[] quantum = {5, 3, 2};

        assertEquals(
                -1,
                scheduler.selectNext(eligible, waiting, quantum)
        );
    }

    @Test
    void shouldReturnMinusOneWhenAllQueuesAreEmpty() {
        boolean[] eligible = {true, true, true};
        int[] waiting = {0, 0, 0};
        int[] quantum = {5, 3, 2};

        assertEquals(
                -1,
                scheduler.selectNext(eligible, waiting, quantum)
        );

        assertEquals(0, scheduler.deficit(0));
        assertEquals(0, scheduler.deficit(1));
        assertEquals(0, scheduler.deficit(2));
    }

    @Test
    void shouldRejectInvalidQueueCount() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new DeficitRoundRobinScheduler(0)
        );
    }

    @Test
    void shouldRejectMismatchedArrayLengths() {
        assertThrows(
                IllegalArgumentException.class,
                () -> scheduler.selectNext(
                        new boolean[]{true},
                        new int[]{1, 2, 3},
                        new int[]{1, 2, 3}
                )
        );
    }

    @Test
    void shouldRejectNegativeWaitingCount() {
        assertThrows(
                IllegalArgumentException.class,
                () -> scheduler.selectNext(
                        new boolean[]{true, true, true},
                        new int[]{1, -1, 1},
                        new int[]{5, 3, 2}
                )
        );
    }

    @Test
    void shouldRejectNonPositiveQuantum() {
        assertThrows(
                IllegalArgumentException.class,
                () -> scheduler.selectNext(
                        new boolean[]{true, true, true},
                        new int[]{1, 1, 1},
                        new int[]{5, 0, 2}
                )
        );
    }

    @Test
    void shouldRejectNullInputs() {
        assertThrows(
                NullPointerException.class,
                () -> scheduler.selectNext(
                        null,
                        new int[]{1, 1, 1},
                        new int[]{1, 1, 1}
                )
        );

        assertThrows(
                NullPointerException.class,
                () -> scheduler.selectNext(
                        new boolean[]{true, true, true},
                        null,
                        new int[]{1, 1, 1}
                )
        );

        assertThrows(
                NullPointerException.class,
                () -> scheduler.selectNext(
                        new boolean[]{true, true, true},
                        new int[]{1, 1, 1},
                        null
                )
        );
    }

    @Test
    void shouldRejectInvalidQueueIndexForDeficitLookup() {
        assertThrows(
                IllegalArgumentException.class,
                () -> scheduler.deficit(-1)
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> scheduler.deficit(3)
        );
    }

    @Test
    void shouldResetAllDeficitsAndCursor() {
        boolean[] eligible = {true, true, true};
        int[] waiting = {100, 100, 100};
        int[] quantum = {5, 3, 2};

        int selected = scheduler.selectNext(
                eligible, waiting, quantum
        );
        scheduler.commitDispatch(selected);

        selected = scheduler.selectNext(
                eligible, waiting, quantum
        );
        scheduler.commitDispatch(selected);

        scheduler.reset();

        assertEquals(0, scheduler.deficit(0));
        assertEquals(0, scheduler.deficit(1));
        assertEquals(0, scheduler.deficit(2));

        selected = scheduler.selectNext(
                eligible, waiting, quantum
        );

        assertEquals(0, selected);

        scheduler.cancelSelection(selected);
    }

    @Test
    void shouldNotConsumeDeficitUntilDispatchIsCommitted() {
        boolean[] eligible = {true, true, true};
        int[] waiting = {100, 100, 100};
        int[] quantum = {5, 3, 2};

        int selected = scheduler.selectNext(
                eligible, waiting, quantum
        );

        assertEquals(0, selected);
        assertEquals(0, scheduler.deficit(0));

        scheduler.commitDispatch(selected);

        assertEquals(4, scheduler.deficit(0));
    }

    @Test
    void shouldNotConsumeDeficitWhenSelectionIsCancelled() {
        boolean[] eligible = {true, true, true};
        int[] waiting = {100, 100, 100};
        int[] quantum = {5, 3, 2};

        int selected = scheduler.selectNext(
                eligible, waiting, quantum
        );

        assertEquals(0, selected);

        scheduler.cancelSelection(selected);

        assertEquals(0, scheduler.deficit(0));

        selected = scheduler.selectNext(
                eligible, waiting, quantum
        );

        assertEquals(0, selected);

        scheduler.cancelSelection(selected);
    }

    @Test
    void shouldRejectAnotherSelectionWhileOneIsPending() {
        boolean[] eligible = {true, true, true};
        int[] waiting = {100, 100, 100};
        int[] quantum = {5, 3, 2};

        scheduler.selectNext(eligible, waiting, quantum);

        assertThrows(
                IllegalStateException.class,
                () -> scheduler.selectNext(
                        eligible, waiting, quantum
                )
        );

        scheduler.cancelSelection(0);
    }

    @Test
    void shouldRejectCommitForWrongQueue() {
        boolean[] eligible = {true, true, true};
        int[] waiting = {100, 100, 100};
        int[] quantum = {5, 3, 2};

        scheduler.selectNext(eligible, waiting, quantum);

        assertThrows(
                IllegalArgumentException.class,
                () -> scheduler.commitDispatch(1)
        );

        scheduler.cancelSelection(0);
    }
}
