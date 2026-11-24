package io.flowgate.internal.dispatch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DynamicQuantumTest {

    private DeficitRoundRobinScheduler scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new DeficitRoundRobinScheduler(2);
    }

    @Test
    void shouldPreserveExistingDeficitWhenQuantumIncreases() {
        boolean[] eligible = {true, true};
        int[] waiting = {100, 100};

        // Q0 receives quantum 4 and successfully dispatches one task.
        int selected = scheduler.selectNext(eligible, waiting, new int[]{4, 1});
        assertEquals(0, selected);
        scheduler.commitDispatch(selected);

        assertEquals(3, scheduler.deficit(0));

        // Quantum increases to 8. Existing deficit remains available.
        assertEquals(3, scheduler.deficit(0));

        selected = scheduler.selectNext(eligible, waiting, new int[]{8, 1});
        assertEquals(0, selected);
        scheduler.commitDispatch(selected);

        assertEquals(2, scheduler.deficit(0));
    }

    @Test
    void shouldPreserveExistingDeficitWhenQuantumDecreases() {
        boolean[] eligible = {true, true};
        int[] waiting = {100, 100};

        // Q0 receives quantum 8 and successfully dispatches one task.
        int selected = scheduler.selectNext(eligible, waiting, new int[]{8, 1});
        assertEquals(0, selected);
        scheduler.commitDispatch(selected);

        assertEquals(7, scheduler.deficit(0));

        // Quantum decreases to 2. Existing deficit is not discarded.
        selected = scheduler.selectNext(eligible, waiting, new int[]{2, 1});
        assertEquals(0, selected);
        scheduler.commitDispatch(selected);

        assertEquals(6, scheduler.deficit(0));
    }

    @Test
    void shouldApplyNewQuantumAfterExistingDeficitIsExhausted() {
        boolean[] eligible = {true, true};
        int[] waiting = {100, 100};

        // Initial quantum is 3. Each selection represents a dispatch.
        for (int i = 0; i < 3; i++) {
            int selected = scheduler.selectNext(eligible, waiting, new int[]{3, 1});
            assertEquals(0, selected);
            scheduler.commitDispatch(selected);
        }

        assertEquals(0, scheduler.deficit(0));

        // Cursor has advanced to Q1.
        int selected = scheduler.selectNext(eligible, waiting, new int[]{6, 1});
        assertEquals(1, selected);
        scheduler.commitDispatch(selected);

        // Cursor returns to Q0. Latest quantum (6) is loaded.
        selected = scheduler.selectNext(eligible, waiting, new int[]{6, 1});
        assertEquals(0, selected);
        scheduler.commitDispatch(selected);

        assertEquals(5, scheduler.deficit(0));
    }

    @Test
    void shouldResetDeficitWhenQueueBecomesEmpty() {
        boolean[] eligible = {true, true};

        int selected = scheduler.selectNext(
                eligible,
                new int[]{100, 100},
                new int[]{5, 1}
        );
        assertEquals(0, selected);
        scheduler.commitDispatch(selected);

        assertEquals(4, scheduler.deficit(0));

        // Queue 0 becomes empty.
        // Q1 is selected for inspection, not dispatched.
        selected = scheduler.selectNext(
                eligible,
                new int[]{0, 100},
                new int[]{5, 1}
        );
        assertEquals(1, selected);
        scheduler.cancelSelection(selected);

        assertEquals(0, scheduler.deficit(0));
    }

    @Test
    void shouldPreserveDeficitWhenQueueIsTemporarilyIneligible() {
        int selected = scheduler.selectNext(
                new boolean[]{true, true},
                new int[]{100, 100},
                new int[]{5, 1}
        );
        assertEquals(0, selected);
        scheduler.commitDispatch(selected);

        assertEquals(4, scheduler.deficit(0));

        // Q0 becomes temporarily ineligible.
        // Q1 is selected for inspection, not dispatched.
        selected = scheduler.selectNext(
                new boolean[]{false, true},
                new int[]{100, 100},
                new int[]{5, 1}
        );
        assertEquals(1, selected);
        scheduler.cancelSelection(selected);

        assertEquals(4, scheduler.deficit(0));
    }

    @Test
    void shouldClearAllDeficitsAndResetCursor() {
        int selected = scheduler.selectNext(
                new boolean[]{true, true},
                new int[]{100, 100},
                new int[]{5, 2}
        );
        scheduler.commitDispatch(selected);

        scheduler.reset();

        assertEquals(0, scheduler.deficit(0));
        assertEquals(0, scheduler.deficit(1));

        selected = scheduler.selectNext(
                new boolean[]{true, true},
                new int[]{100, 100},
                new int[]{5, 2}
        );

        assertEquals(0, selected);
        scheduler.cancelSelection(selected);
    }
}