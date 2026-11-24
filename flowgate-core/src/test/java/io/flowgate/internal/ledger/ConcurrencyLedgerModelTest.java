
package io.flowgate.internal.ledger;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class ConcurrencyLedgerModelTest {

    private static final int GLOBAL_LIMIT = 12;

    private static final int[] RESERVED = {2, 1, 2, 0};
    private static final int[] MAXIMUM = {6, 5, 4, 3};

    private static final int OPERATIONS_PER_SEED = 10_000;

    private record HeldPermit(
            int queueIndex,
            ExecutionPermit permit
    ) {}

    @Test
    void matchesReferenceModelAcrossRandomSequences() {

        long[] seeds = {
                42L,
                12345L,
                987654L,
                20261114L,
                8675309L
        };

        for (long seed : seeds) {
            runRandomSequence(seed);
        }
    }

    private void runRandomSequence(long seed) {

        ConcurrencyLedger ledger = new ConcurrencyLedger(
                GLOBAL_LIMIT,
                RESERVED,
                MAXIMUM
        );

        ReferenceModel model = new ReferenceModel(
                GLOBAL_LIMIT,
                RESERVED,
                MAXIMUM
        );

        List<HeldPermit> permits = new ArrayList<>();
        Random random = new Random(seed);

        for (int operation = 0;
             operation < OPERATIONS_PER_SEED;
             operation++) {

            int queueIndex = random.nextInt(RESERVED.length);
            int[] waiting = randomWaitingSnapshot(random);

            if (permits.isEmpty() || random.nextInt(100) < 65) {

                verifyAcquisition(
                        ledger,
                        model,
                        permits,
                        queueIndex,
                        waiting,
                        seed,
                        operation
                );

            } else {

                releaseRandomPermit(
                        permits,
                        model,
                        random
                );
            }

            assertLedgerMatchesModel(
                    ledger,
                    model,
                    seed,
                    operation
            );
        }

        // Release all remaining permits.
        for (HeldPermit held : permits) {
            held.permit().close();
            model.release(held.queueIndex());
        }

        permits.clear();

        assertLedgerMatchesModel(
                ledger,
                model,
                seed,
                OPERATIONS_PER_SEED
        );

        assertEquals(0, ledger.activeTotal());
        assertEquals(GLOBAL_LIMIT, ledger.available());
    }

    private void verifyAcquisition(
            ConcurrencyLedger ledger,
            ReferenceModel model,
            List<HeldPermit> permits,
            int queueIndex,
            int[] waiting,
            long seed,
            int operation
    ) {

        String context = context(seed, operation);

        AllocationDecision expected =
                model.decide(queueIndex, waiting);

        AllocationDecision actual =
                ledger.check(queueIndex, waiting);

        assertEquals(
                expected,
                actual,
                context + ": allocation decision mismatch"
        );

        ExecutionPermit permit =
                ledger.tryAcquire(queueIndex, waiting);

        if (expected.isGranted()) {

            assertNotNull(
                    permit,
                    context + ": expected permit"
            );

            assertEquals(
                    expected == AllocationDecision.GRANT_BORROWED,
                    permit.wasBorrowed(),
                    context + ": borrowed classification mismatch"
            );

            permits.add(new HeldPermit(queueIndex, permit));
            model.acquire(queueIndex);

        } else {

            assertNull(
                    permit,
                    context + ": unexpected permit"
            );
        }
    }

    private void releaseRandomPermit(
            List<HeldPermit> permits,
            ReferenceModel model,
            Random random
    ) {

        int index = random.nextInt(permits.size());

        HeldPermit held = permits.remove(index);

        held.permit().close();
        model.release(held.queueIndex());
    }

    private int[] randomWaitingSnapshot(Random random) {

        int[] waiting = new int[RESERVED.length];

        for (int i = 0; i < waiting.length; i++) {
            waiting[i] = random.nextInt(7);
        }

        return waiting;
    }

    private void assertLedgerMatchesModel(
            ConcurrencyLedger ledger,
            ReferenceModel model,
            long seed,
            int operation
    ) {

        String context = context(seed, operation);

        assertEquals(
                model.totalActive(),
                ledger.activeTotal(),
                context + ": total active mismatch"
        );

        assertEquals(
                GLOBAL_LIMIT - model.totalActive(),
                ledger.available(),
                context + ": available capacity mismatch"
        );

        assertTrue(
                ledger.activeTotal() <= GLOBAL_LIMIT,
                context + ": global limit exceeded"
        );

        for (int i = 0; i < RESERVED.length; i++) {

            assertEquals(
                    model.active(i),
                    ledger.active(i),
                    context + ": active count mismatch for queue " + i
            );

            assertEquals(
                    Math.max(0, model.active(i) - RESERVED[i]),
                    ledger.borrowed(i),
                    context + ": borrowed count mismatch for queue " + i
            );

            assertTrue(
                    ledger.active(i) <= MAXIMUM[i],
                    context + ": maximum exceeded for queue " + i
            );

            assertTrue(
                    ledger.active(i) >= 0,
                    context + ": negative active count for queue " + i
            );
        }
    }

    private String context(long seed, int operation) {
        return "seed=" + seed + ", operation=" + operation;
    }

    /**
     * Independent reference implementation of the allocation policy.
     *
     * Does not call ConcurrencyLedger or reuse its decision method.
     */
    private static final class ReferenceModel {

        private final int globalLimit;
        private final int[] reserved;
        private final int[] maximum;
        private final int[] active;

        private int totalActive;

        private ReferenceModel(
                int globalLimit,
                int[] reserved,
                int[] maximum
        ) {
            this.globalLimit = globalLimit;
            this.reserved = reserved.clone();
            this.maximum = maximum.clone();
            this.active = new int[reserved.length];
        }

        private AllocationDecision decide(
                int queueIndex,
                int[] waiting
        ) {

            int free = globalLimit - totalActive;

            if (free <= 0) {
                return AllocationDecision.DENY_GLOBAL_LIMIT;
            }

            if (active[queueIndex] >= maximum[queueIndex]) {
                return AllocationDecision.DENY_QUEUE_MAX;
            }

            if (active[queueIndex] < reserved[queueIndex]) {
                return AllocationDecision.GRANT_RESERVED;
            }

            long protectedCapacity = 0;

            for (int i = 0; i < active.length; i++) {

                if (i == queueIndex) {
                    continue;
                }

                int unmetReservation = Math.max(
                        0,
                        reserved[i] - active[i]
                );

                int waitingDemand = Math.max(0, waiting[i]);

                protectedCapacity += Math.min(
                        unmetReservation,
                        waitingDemand
                );
            }

            if (free > protectedCapacity) {
                return AllocationDecision.GRANT_BORROWED;
            }

            return AllocationDecision.DENY_RESERVED_FOR_OTHERS;
        }

        private void acquire(int queueIndex) {
            active[queueIndex]++;
            totalActive++;
        }

        private void release(int queueIndex) {

            if (active[queueIndex] <= 0 || totalActive <= 0) {
                throw new IllegalStateException(
                        "Reference model accounting underflow"
                );
            }

            active[queueIndex]--;
            totalActive--;
        }

        private int active(int queueIndex) {
            return active[queueIndex];
        }

        private int totalActive() {
            return totalActive;
        }
    }
}
