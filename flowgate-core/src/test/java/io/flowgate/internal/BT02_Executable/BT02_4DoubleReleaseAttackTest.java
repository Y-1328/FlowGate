package io.flowgate.internal.BT02_Executable;

import io.flowgate.internal.ledger.ConcurrencyLedger;
import io.flowgate.internal.ledger.ExecutionPermit;
import org.junit.jupiter.api.Test;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
class BT02_4DoubleReleaseAttackTest {
    @Test void repeatedConcurrentCloseReleasesExactlyOnce() throws Exception {
        int global = 1;
        ConcurrencyLedger ledger = new ConcurrencyLedger(global, new int[]{1}, new int[]{1});
        ExecutionPermit permit = ledger.tryAcquire(0, new int[]{0});
        assertNotNull(permit); assertEquals(1, ledger.activeTotal());
        int threads = 100;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads), start = new CountDownLatch(1);
        Future<?>[] futures = new Future<?>[threads];
        try {
            for (int i = 0; i < threads; i++) futures[i] = pool.submit(() -> { ready.countDown(); BT02TestSupport.await(start); permit.close(); });
            assertTrue(ready.await(20, TimeUnit.SECONDS)); start.countDown();
            for (Future<?> f : futures) f.get(20, TimeUnit.SECONDS);
        } finally { BT02TestSupport.shutdown(pool); }
        assertTrue(permit.isReleased());
        assertEquals(0, ledger.activeTotal(), "Double close must not decrement ledger twice");
        assertEquals(1, ledger.available());
        ExecutionPermit replacement = ledger.tryAcquire(0, new int[]{0});
        assertNotNull(replacement); replacement.close();
        assertEquals(0, ledger.activeTotal());
    }
}
