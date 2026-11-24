package io.flowgate.internal.FlowGate_Core_Bleed_BT01.flowgate.internal.dispatch;


import io.flowgate.internal.dispatch.DeficitRoundRobinScheduler;
import org.junit.jupiter.api.Test;
import java.util.Random;
import static org.junit.jupiter.api.Assertions.*;
/** BT-01E: deterministic randomized scheduler input churn. */
class DeficitRoundRobinBleedTest {
    @Test
    void randomizedEligibleWaitingAndQuantumPatternsRemainConsistent() {
        final int queues = 7, rounds = 250_000;
        Random random = new Random(0xF10A6E);
        DeficitRoundRobinScheduler scheduler = new DeficitRoundRobinScheduler(queues);
        for (int round = 0; round < rounds; round++) {
            boolean[] eligible = new boolean[queues];
            int[] waiting = new int[queues];
            int[] quantum = new int[queues];
            for (int q = 0; q < queues; q++) {
                eligible[q] = random.nextBoolean();
                waiting[q] = random.nextInt(0, 100);
                quantum[q] = random.nextInt(1, 100);
            }
            int selected = scheduler.selectNext(eligible, waiting, quantum);
            if (selected == -1) {
                for (int q = 0; q < queues; q++) assertFalse(eligible[q] && waiting[q] > 0);
            } else {
                assertTrue(selected >= 0 && selected < queues);
                assertTrue(eligible[selected]);
                assertTrue(waiting[selected] > 0);
                if (random.nextInt(5) == 0) scheduler.cancelSelection(selected);
                else scheduler.commitDispatch(selected);
            }
            for (int q = 0; q < queues; q++) assertTrue(scheduler.deficit(q) >= 0);
        }
    }
}
