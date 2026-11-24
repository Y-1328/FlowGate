package io.flowgate.internal.FlowGate_Core_Bleed_BT01.flowgate.internal.queue;


import io.flowgate.api.Priority;
import io.flowgate.config.QueueConfig;
import io.flowgate.internal.queue.GlobalAdmissionGate;
import io.flowgate.internal.queue.WorkloadQueue;
import org.junit.jupiter.api.Test;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

/** BT-01B: Concurrent local queue-slot reservation and rollback pressure. */
class WorkloadQueueBleedTest {
    @Test
    void concurrentReservationsNeverExceedLocalOrGlobalCapacity() throws Exception {
        final int capacity = 32, threads = 64, attempts = 5_000;
        GlobalAdmissionGate gate = new GlobalAdmissionGate(capacity);
        QueueConfig config = new QueueConfig("bleed", Priority.NORMAL, 1, capacity, 0, 1, null);
        WorkloadQueue queue = new WorkloadQueue(0, config, gate);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads), start = new CountDownLatch(1);
        java.util.List<Future<?>> futures = new java.util.ArrayList<>();
        AtomicInteger peakQueueSize = new AtomicInteger();
        AtomicInteger reservations = new AtomicInteger();
        try {
            for (int t = 0; t < threads; t++) futures.add(pool.submit(() -> {
                ready.countDown();
                try { start.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
                for (int i = 0; i < attempts; i++) {
                    WorkloadQueue.Reservation result = queue.tryReserve();
                    if (result == WorkloadQueue.Reservation.RESERVED) {
                        reservations.incrementAndGet();
                        int observed = queue.size();
                        peakQueueSize.accumulateAndGet(observed, Math::max);
                        assertTrue(observed <= capacity, "queue slot counter exceeded capacity");
                        queue.abortReservation();
                    }
                }
            }));
            assertTrue(ready.await(20, TimeUnit.SECONDS));
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(90, TimeUnit.SECONDS));
            for (Future<?> f : futures) f.get();
            assertTrue(reservations.get() > 0, "attack should acquire some reservations");
            assertTrue(peakQueueSize.get() <= capacity);
            assertEquals(0, queue.size(), "all reservations should have been rolled back");
            assertEquals(0, gate.held(), "global gate accounting should return to zero");
        } finally { start.countDown(); pool.shutdownNow(); }
    }
}
