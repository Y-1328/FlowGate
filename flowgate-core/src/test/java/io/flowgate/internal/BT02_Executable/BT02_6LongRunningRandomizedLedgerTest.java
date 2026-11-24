package io.flowgate.internal.BT02_Executable;

import io.flowgate.internal.ledger.ConcurrencyLedger;
import io.flowgate.internal.ledger.ExecutionPermit;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
class BT02_6LongRunningRandomizedLedgerTest {
    @Test void seededRandomAcquireReleaseReconcileAfterEachWorkerCompletes() throws Exception {
        int global = 16, queues = 4, threads = 40, operationsPerThread = 50_000;
        int[] reservations = {2,2,2,2}, maximum = {16,16,16,16};
        ConcurrencyLedger ledger = new ConcurrencyLedger(global, reservations, maximum);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads), start = new CountDownLatch(1);
        Future<?>[] futures = new Future<?>[threads];
        try {
            for (int t = 0; t < threads; t++) {
                final int queue = t % queues, seed = 9000 + t;
                futures[t] = pool.submit(() -> {
                    Random random = new Random(seed);
                    ArrayDeque<ExecutionPermit> local = new ArrayDeque<>();
                    ready.countDown(); BT02TestSupport.await(start);
                    try {
                        for (int i = 0; i < operationsPerThread; i++) {
                            if (!local.isEmpty() && random.nextInt(100) < 48) {
                                ExecutionPermit p = local.removeFirst(); p.close();
                            } else {
                                ExecutionPermit p = ledger.tryAcquire(queue, new int[]{0,0,0,0});
                                if (p != null) local.addLast(p);
                            }
                            if ((i & 1023) == 0) Thread.yield();
                        }
                    } finally { while (!local.isEmpty()) local.removeFirst().close(); }
                });
            }
            assertTrue(ready.await(20, TimeUnit.SECONDS)); start.countDown();
            for (Future<?> f : futures) f.get(120, TimeUnit.SECONDS);
        } finally { BT02TestSupport.shutdown(pool); }
        assertEquals(0, ledger.activeTotal(), "All worker-held permits must be closed");
        assertEquals(global, ledger.available());
        for (int q = 0; q < queues; q++) assertEquals(0, ledger.active(q));
        BT02TestSupport.assertLedgerConsistent(ledger, queues, global);
    }
}
