package io.flowgate.internal.ledger;

import org.junit.jupiter.api.Test;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;
class BT02_3AcquireReleaseRaceTest {
    @Test void randomizedAcquireCloseRacesPreserveAccounting() throws Exception {
        int global = 10, queues = 5, threads = 48, loops = 20_000;
        ConcurrencyLedger ledger = new ConcurrencyLedger(global, new int[]{1,1,1,1,1}, new int[]{10,10,10,10,10});
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads), start = new CountDownLatch(1);
        AtomicInteger successful = new AtomicInteger();
        Future<?>[] futures = new Future<?>[threads];
        try {
            for (int t = 0; t < threads; t++) {
                final int q = t % queues, seed = 1328 + t;
                futures[t] = pool.submit(() -> {
                    java.util.Random random = new java.util.Random(seed);
                    ready.countDown(); BT02TestSupport.await(start);
                    for (int i = 0; i < loops; i++) {
                        ExecutionPermit p = ledger.tryAcquire(q, new int[]{0,0,0,0,0});
                        if (p != null) {
                            successful.incrementAndGet();
                            if (random.nextBoolean()) Thread.yield();
                            p.close();
                            if (random.nextInt(5) == 0) p.close();
                        } else if ((i & 7) == 0) Thread.yield();
                    }
                });
            }
            assertTrue(ready.await(20, TimeUnit.SECONDS)); start.countDown();
            for (Future<?> f : futures) f.get(60, TimeUnit.SECONDS);
        } finally { BT02TestSupport.shutdown(pool); }
        assertTrue(successful.get() > 0);
        assertEquals(0, ledger.activeTotal());
        BT02TestSupport.assertLedgerConsistent(ledger, queues, global);
    }
}
