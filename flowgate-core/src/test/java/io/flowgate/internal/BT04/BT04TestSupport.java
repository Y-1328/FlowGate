package io.flowgate.internal.BT04;

import io.flowgate.internal.dispatch.DeficitRoundRobinScheduler;

final class BT04TestSupport {

    private BT04TestSupport() {}

    static int[] run(
            DeficitRoundRobinScheduler scheduler,
            int iterations,
            boolean[] eligible,
            int[] waiting,
            int[] quantum) {

        int[] counts = new int[eligible.length];

        for (int i = 0; i < iterations; i++) {
            int selected = scheduler.selectNext(eligible, waiting, quantum);

            if (selected < 0) {
                continue;
            }

            counts[selected]++;
            scheduler.commitDispatch(selected);
        }

        return counts;
    }

    static double ratio(int[] counts, int index) {
        int total = 0;

        for (int count : counts) {
            total += count;
        }

        if (total == 0) {
            return 0.0;
        }

        return (double) counts[index] / total;
    }

    static int total(int[] counts) {
        int total = 0;

        for (int count : counts) {
            total += count;
        }

        return total;
    }
}