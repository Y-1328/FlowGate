package io.flowgate.internal.FlowGate_Core_Bleed_BT01.flowgate.internal.ledger;

import io.flowgate.internal.ledger.ConcurrencyLedger;
import io.flowgate.internal.ledger.ExecutionPermit;
import org.junit.jupiter.api.Test;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

/** BT-01C: Permit accounting under concurrent acquisition/release. */
class ConcurrencyLedgerBleedTest {
    @Test
    void totalActiveMustNeverExceedGlobalConcurrency() throws Exception {
        final int globalLimit = 8, threads = 32, iterations = 4_000;
        ConcurrencyLedger ledger = new ConcurrencyLedger(globalLimit, new int[]{0,0}, new int[]{8,8});
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads), start = new CountDownLatch(1);
        java.util.List<Future<?>> futures = new java.util.ArrayList<>();
        AtomicInteger observedPeak = new AtomicInteger();
        try {
            for (int t = 0; t < threads; t++) {
                final int queueIndex = t % 2;
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    try { start.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
                    for (int i = 0; i < iterations; i++) {
                        ExecutionPermit permit = ledger.tryAcquire(queueIndex, new int[]{100,100});
                        if (permit != null) {
                            int active = ledger.activeTotal();
                            observedPeak.accumulateAndGet(active, Math::max);
                            assertTrue(active <= globalLimit, "ledger granted beyond global limit");
                            try { Thread.yield(); } finally { permit.close(); permit.close(); }
                        }
                    }
                }));
            }
            assertTrue(ready.await(20, TimeUnit.SECONDS));
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(90, TimeUnit.SECONDS));
            for (Future<?> f : futures) f.get();
            assertTrue(observedPeak.get() > 0);
            assertTrue(observedPeak.get() <= globalLimit);
            assertEquals(0, ledger.activeTotal(), "all permits must be released");
        } finally { start.countDown(); pool.shutdownNow(); }
    }
}
