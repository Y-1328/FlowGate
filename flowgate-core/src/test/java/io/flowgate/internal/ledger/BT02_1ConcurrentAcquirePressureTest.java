package io.flowgate.internal.ledger;


import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;


class BT02_1ConcurrentAcquirePressureTest {
    @Test void simultaneousAcquireNeverExceedsGlobalOrQueueCaps() throws Exception {
        int global = 12, queues = 4, threads = 128;
        int[] reservations = {2, 2, 2, 2}, maximum = {6, 6, 6, 6}, waiting = {100,100,100,100};
        ConcurrencyLedger ledger = new ConcurrencyLedger(global, reservations, maximum);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads), start = new CountDownLatch(1);
        List<ExecutionPermit> held = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger granted = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int t = 0; t < threads; t++) {
                final int q = t % queues;
                futures.add(pool.submit(() -> {
                    ready.countDown(); BT02TestSupport.await(start);
                    ExecutionPermit p = ledger.tryAcquire(q, waiting);
                    if (p != null) { held.add(p); granted.incrementAndGet(); }
                }));
            }
            assertTrue(ready.await(20, TimeUnit.SECONDS)); start.countDown();
            for (Future<?> f : futures) f.get(20, TimeUnit.SECONDS);
            assertEquals(global, granted.get(), "With no reservation-demand protection, all global slots should be grantable");
            assertTrue(ledger.activeTotal() <= global);
            for (int q = 0; q < queues; q++) assertTrue(ledger.active(q) <= maximum[q]);
            BT02TestSupport.assertLedgerConsistent(ledger, queues, global);
        } finally {
            held.forEach(ExecutionPermit::close);
            BT02TestSupport.shutdown(pool);
        }
        assertEquals(0, ledger.activeTotal());
        for (int q = 0; q < queues; q++) assertEquals(0, ledger.active(q));
    }
}
