package io.flowgate.internal.queue;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class BT03_4DrainRefillRaceTest {
    @Test void producersAndConsumerDrainRefillWithoutAccountingDrift() throws Exception {
        int capacity = 64, producers = 6, perProducer = 3000;
        GlobalAdmissionGate gate = new GlobalAdmissionGate(capacity);
        WorkloadQueue q = BT03TestSupport.queue("drain", 0, capacity, gate);
        ExecutorService pool = Executors.newFixedThreadPool(producers + 1);
        CountDownLatch start = new CountDownLatch(1);
        AtomicLong accepted = new AtomicLong(), consumed = new AtomicLong();
        try {
            Future<?> consumer = pool.submit(() -> { start.await(); while (consumed.get() < accepted.get() || !pool.isShutdown()) { if (q.poll() != null) consumed.incrementAndGet(); else Thread.yield(); } return null; });
            Future<?>[] prod = new Future<?>[producers];
            for (int p = 0; p < producers; p++) { final int pId = p; prod[p] = pool.submit(() -> { start.await(); for (int i = 0; i < perProducer; i++) { long id = pId * (long) perProducer + i; if (BT03TestSupport.submit(q, "drain", id)) accepted.incrementAndGet(); else Thread.yield(); } return null; }); }
            start.countDown(); for (Future<?> f : prod) f.get(30, TimeUnit.SECONDS);
            pool.shutdown(); assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS)); consumer.get(2, TimeUnit.SECONDS);
            while (q.poll() != null) consumed.incrementAndGet();
            assertEquals(accepted.get(), consumed.get()); assertEquals(0, q.size()); assertEquals(0, gate.held());
        } finally { pool.shutdownNow(); }
    }
}
