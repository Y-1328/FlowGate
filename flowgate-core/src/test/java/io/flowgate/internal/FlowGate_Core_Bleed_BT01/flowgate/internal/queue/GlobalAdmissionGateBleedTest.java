package io.flowgate.internal.FlowGate_Core_Bleed_BT01.flowgate.internal.queue;

import io.flowgate.internal.queue.GlobalAdmissionGate;
import org.junit.jupiter.api.Test;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;


/** BT-01A: Contended global admission gate. */
class GlobalAdmissionGateBleedTest {
    @Test
    void simultaneousAcquisitionMustNeverExceedLimit() throws Exception {
        final int limit = 64, threads = 80, attemptsPerThread = 20_000;
        GlobalAdmissionGate gate = new GlobalAdmissionGate(limit);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads), start = new CountDownLatch(1);
        java.util.List<Future<?>> futures = new java.util.ArrayList<>();
        AtomicInteger successfulAcquires = new AtomicInteger();
        try {
            for (int t = 0; t < threads; t++) futures.add(pool.submit(() -> {
                ready.countDown();
                try { start.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
                for (int i = 0; i < attemptsPerThread; i++) {
                    if (gate.tryAcquire() == GlobalAdmissionGate.Result.ACQUIRED) successfulAcquires.incrementAndGet();
                    assertTrue(gate.held() <= limit, "held count exceeded configured limit");
                }
            }));
            assertTrue(ready.await(20, TimeUnit.SECONDS));
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(90, TimeUnit.SECONDS));
            for (Future<?> f : futures) f.get();
            assertEquals(limit, successfulAcquires.get(), "Gate should grant exactly its capacity while permits are retained");
            assertEquals(limit, gate.held());
            for (int i = 0; i < limit; i++) gate.release();
            assertEquals(0, gate.held());
        } finally { start.countDown(); pool.shutdownNow(); }
    }

    @Test
    void closeMustRaceLinearlyWithAcquireAndNeverReopen() throws Exception {
        GlobalAdmissionGate gate = new GlobalAdmissionGate(10_000);
        ExecutorService pool = Executors.newFixedThreadPool(16);
        CountDownLatch start = new CountDownLatch(1);
        try {
            for (int i = 0; i < 15; i++) pool.submit(() -> {
                try { start.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
                for (int j = 0; j < 100_000; j++) gate.tryAcquire();
            });
            pool.submit(() -> { try { start.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; } gate.close(); });
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(90, TimeUnit.SECONDS));
            assertTrue(gate.isClosed());
            assertEquals(GlobalAdmissionGate.Result.CLOSED, gate.tryAcquire());
        } finally { start.countDown(); pool.shutdownNow(); }
    }
}
