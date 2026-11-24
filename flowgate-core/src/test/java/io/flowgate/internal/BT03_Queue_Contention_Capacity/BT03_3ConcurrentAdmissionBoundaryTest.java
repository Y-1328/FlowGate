package io.flowgate.internal.queue;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class BT03_3ConcurrentAdmissionBoundaryTest {
    @Test void simultaneousSubmittersRespectCapacity() throws Exception {
        int capacity = 24, threads = 120;
        GlobalAdmissionGate gate = new GlobalAdmissionGate(capacity);
        WorkloadQueue q = BT03TestSupport.queue("race", 0, capacity, gate);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads), start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        try {
            for (int i = 0; i < threads; i++) {
                final int id = i;
                pool.submit(() -> { ready.countDown(); start.await(); if (BT03TestSupport.submit(q, "race", id)) accepted.incrementAndGet(); return null; });
            }
            assertTrue(ready.await(10, TimeUnit.SECONDS)); start.countDown();
            pool.shutdown(); assertTrue(pool.awaitTermination(20, TimeUnit.SECONDS));
            assertEquals(capacity, accepted.get());
            assertEquals(capacity, q.size());
            assertEquals(capacity, gate.held());
        } finally { pool.shutdownNow(); }
    }
}
