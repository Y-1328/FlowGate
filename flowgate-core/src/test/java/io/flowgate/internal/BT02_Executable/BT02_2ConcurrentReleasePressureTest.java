package io.flowgate.internal.BT02_Executable;

import io.flowgate.internal.ledger.ConcurrencyLedger;
import io.flowgate.internal.ledger.ExecutionPermit;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class BT02_2ConcurrentReleasePressureTest {
    @Test void manyPermitsReleasedSimultaneouslyRestoreAllCapacity() throws Exception {
        int global = 32, queues = 4;
        ConcurrencyLedger ledger = new ConcurrencyLedger(global, new int[]{8,8,8,8}, new int[]{32,32,32,32});
        List<ExecutionPermit> permits = new ArrayList<>();
        for (int q = 0; q < queues; q++) for (int i = 0; i < 8; i++) {
            ExecutionPermit p = ledger.tryAcquire(q, new int[]{0,0,0,0});
            assertNotNull(p); permits.add(p);
        }
        assertEquals(global, ledger.activeTotal());
        ExecutorService pool = Executors.newFixedThreadPool(64);
        CountDownLatch ready = new CountDownLatch(permits.size()), start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (ExecutionPermit p : permits) futures.add(pool.submit(() -> { ready.countDown(); BT02TestSupport.await(start); p.close(); }));
            assertTrue(ready.await(20, TimeUnit.SECONDS)); start.countDown();
            for (Future<?> f : futures) f.get(20, TimeUnit.SECONDS);
        } finally { BT02TestSupport.shutdown(pool); }
        assertEquals(0, ledger.activeTotal());
        assertEquals(global, ledger.available());
        for (int q = 0; q < queues; q++) assertEquals(0, ledger.active(q));
        BT02TestSupport.assertLedgerConsistent(ledger, queues, global);
    }
}
