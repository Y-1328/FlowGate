package io.flowgate.internal.BT02_Executable;

import io.flowgate.internal.ledger.ConcurrencyLedger;
import io.flowgate.internal.ledger.ExecutionPermit;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
class BT02_5ReservationBorrowingContentionTest {
    @Test void borrowedCapacityCannotConsumeObservedUnmetReservations() throws Exception {
        int global = 8, threads = 80;
        int[] reservations = {2,2,2,2}, max = {8,8,8,8}, waiting = {0,20,20,20};
        ConcurrencyLedger ledger = new ConcurrencyLedger(global, reservations, max);
        List<ExecutionPermit> held = Collections.synchronizedList(new ArrayList<>());
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads), start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int t = 0; t < threads; t++) futures.add(pool.submit(() -> {
                ready.countDown(); BT02TestSupport.await(start);
                ExecutionPermit p = ledger.tryAcquire(0, waiting);
                if (p != null) held.add(p);
            }));
            assertTrue(ready.await(20, TimeUnit.SECONDS)); start.countDown();
            for (Future<?> f : futures) f.get(20, TimeUnit.SECONDS);
            assertTrue(ledger.active(0) <= 2, "Queue 0 must not borrow capacity protected for other queues with waiting demand");
            assertEquals(0, ledger.borrowed(0));
            BT02TestSupport.assertLedgerConsistent(ledger, 4, global);
        } finally { held.forEach(ExecutionPermit::close); BT02TestSupport.shutdown(pool); }
        assertEquals(0, ledger.activeTotal());
    }
}
