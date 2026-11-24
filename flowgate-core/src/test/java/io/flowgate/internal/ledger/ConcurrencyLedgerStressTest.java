
package io.flowgate.internal.ledger;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ConcurrencyLedgerStressTest {

    private static final int GLOBAL_LIMIT = 12;
    private static final int THREADS = 32;

    private static final int[] RESERVED = {0, 0, 0, 0};
    private static final int[] MAXIMUM = {12, 12, 12, 12};
    private static final int[] WAITING = {100, 100, 100, 100};

    @Test
    void neverGrantsMorePermitsThanGlobalConcurrency()
            throws Exception {

        ConcurrencyLedger ledger = new ConcurrencyLedger(
                GLOBAL_LIMIT,
                RESERVED,
                MAXIMUM
        );

        ExecutorService executor =
                Executors.newFixedThreadPool(THREADS);

        CountDownLatch ready = new CountDownLatch(THREADS);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch attemptsCompleted = new CountDownLatch(THREADS);
        CountDownLatch releasePermits = new CountDownLatch(1);

        AtomicInteger granted = new AtomicInteger();

        List<Future<?>> futures = new ArrayList<>();

        try {
            for (int worker = 0; worker < THREADS; worker++) {

                final int queueIndex = worker % RESERVED.length;

                futures.add(executor.submit(() -> {

                    ready.countDown();

                    ExecutionPermit permit = null;

                    try {
                        // All workers begin acquisition together.
                        if (!start.await(10, TimeUnit.SECONDS)) {
                            throw new AssertionError(
                                    "Start signal timed out"
                            );
                        }

                        permit = ledger.tryAcquire(
                                queueIndex,
                                WAITING
                        );

                        if (permit != null) {
                            granted.incrementAndGet();
                        }

                    } finally {
                        attemptsCompleted.countDown();
                    }

                    // Successful workers retain their permits.
                    // This lets the main test inspect the ledger
                    // while all granted permits are still active.
                    if (permit != null) {
                        try {
                            if (!releasePermits.await(
                                    15, TimeUnit.SECONDS)) {
                                throw new AssertionError(
                                        "Permit release timed out"
                                );
                            }
                        } finally {
                            permit.close();
                        }
                    }

                    return null;
                }));
            }

            assertTrue(
                    ready.await(10, TimeUnit.SECONDS),
                    "Not all workers became ready"
            );

            // Release all 32 workers simultaneously.
            start.countDown();

            assertTrue(
                    attemptsCompleted.await(15, TimeUnit.SECONDS),
                    "Workers did not complete acquisition attempts"
            );

            // Exactly 12 permits should have been granted.
            assertEquals(GLOBAL_LIMIT, granted.get());

            assertEquals(GLOBAL_LIMIT, ledger.activeTotal());

            assertEquals(
                    0,
                    ledger.available(),
                    "Global capacity should be fully occupied"
            );

            assertTrue(
                    ledger.activeTotal() <= GLOBAL_LIMIT,
                    "Global concurrency limit exceeded"
            );

            for (int q = 0; q < ledger.queueCount(); q++) {
                assertTrue(
                        ledger.active(q) <= MAXIMUM[q],
                        "Queue maximum exceeded: " + q
                );
            }

        } finally {
            // Never leave workers blocked, even if an assertion fails.
            start.countDown();
            releasePermits.countDown();

            executor.shutdown();

            assertTrue(
                    executor.awaitTermination(20, TimeUnit.SECONDS),
                    "Executor did not terminate"
            );
        }

        for (Future<?> future : futures) {
            future.get();
        }

        assertEquals(0, ledger.activeTotal());
        assertEquals(GLOBAL_LIMIT, ledger.available());
    }
}
