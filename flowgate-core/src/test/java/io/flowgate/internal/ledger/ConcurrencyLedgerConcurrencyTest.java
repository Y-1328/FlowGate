
package io.flowgate.internal.ledger;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.junit.jupiter.api.Assertions.*;

class ConcurrencyLedgerConcurrencyTest {

    private static final int THREADS = 16;
    private static final int ITERATIONS = 5_000;

    @Test
    void concurrentAcquisitionsNeverExceedConcurrencyLimits() throws Exception {
        int globalLimit = 8;
        int queueCount = 4;

        int[] reserved = {2, 2, 2, 2};
        int[] maximum = {8, 8, 8, 8};

        ConcurrencyLedger ledger =
                new ConcurrencyLedger(globalLimit, reserved, maximum);

        AtomicInteger maximumObservedGlobal = new AtomicInteger();
        AtomicIntegerArray maximumObservedPerQueue =
                new AtomicIntegerArray(queueCount);

        AtomicInteger successfulAcquisitions = new AtomicInteger();

        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        CountDownLatch ready = new CountDownLatch(THREADS);
        CountDownLatch start = new CountDownLatch(1);

        try {
            Future<?>[] futures = new Future<?>[THREADS];

            for (int thread = 0; thread < THREADS; thread++) {
                final int queueIndex = thread % queueCount;

                futures[thread] = executor.submit(() -> {
                    ready.countDown();

                    if (!start.await(10, TimeUnit.SECONDS)) {
                        throw new AssertionError("Start barrier timed out");
                    }

                    int[] waitingSnapshot = new int[queueCount];

                    for (int i = 0; i < ITERATIONS; i++) {
                        ExecutionPermit permit =
                                ledger.tryAcquire(queueIndex, waitingSnapshot);

                        if (permit == null) {
                            Thread.yield();
                            continue;
                        }

                        successfulAcquisitions.incrementAndGet();

                        maximumObservedGlobal.accumulateAndGet(
                                ledger.activeTotal(),
                                Math::max
                        );

                        maximumObservedPerQueue.accumulateAndGet(
                                queueIndex,
                                ledger.active(queueIndex),
                                Math::max
                        );

                        if ((i & 15) == 0) {
                            Thread.yield();
                        }

                        permit.close();
                    }

                    return null;
                });
            }

            assertTrue(ready.await(10, TimeUnit.SECONDS));
            start.countDown();

            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }

        } finally {
            start.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }

        assertTrue(successfulAcquisitions.get() > 0);

        assertTrue(
                maximumObservedGlobal.get() <= globalLimit,
                "Global concurrency limit was exceeded"
        );

        for (int i = 0; i < queueCount; i++) {
            assertTrue(
                    maximumObservedPerQueue.get(i) <= maximum[i],
                    "Queue " + i + " exceeded its maximum concurrency"
            );
        }

        assertEquals(0, ledger.activeTotal());
        assertEquals(globalLimit, ledger.available());
    }

    @Test
    void concurrentCloseReleasesPermitExactlyOnce() throws Exception {
        ConcurrencyLedger ledger =
                new ConcurrencyLedger(4, new int[]{1}, new int[]{4});

        ExecutionPermit permit = ledger.tryAcquire(0, new int[]{0});

        assertNotNull(permit);
        assertEquals(1, ledger.activeTotal());

        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        CountDownLatch ready = new CountDownLatch(THREADS);
        CountDownLatch start = new CountDownLatch(1);

        try {
            Future<?>[] futures = new Future<?>[THREADS];

            for (int i = 0; i < THREADS; i++) {
                futures[i] = executor.submit(() -> {
                    ready.countDown();

                    if (!start.await(10, TimeUnit.SECONDS)) {
                        throw new AssertionError("Start barrier timed out");
                    }

                    permit.close();
                    return null;
                });
            }

            assertTrue(ready.await(10, TimeUnit.SECONDS));
            start.countDown();

            for (Future<?> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }

        } finally {
            start.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }

        assertTrue(permit.isReleased());
        assertEquals(0, ledger.activeTotal());
        assertEquals(0, ledger.active(0));
        assertEquals(4, ledger.available());
    }
}
