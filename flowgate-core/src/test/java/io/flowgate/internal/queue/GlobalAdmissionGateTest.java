package io.flowgate.internal.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.flowgate.internal.queue.GlobalAdmissionGate.Result;
import io.flowgate.testsupport.Concurrently;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class GlobalAdmissionGateTest {

    @Test
    void acquiresUpToLimitThenReportsLimit() {
        GlobalAdmissionGate gate = new GlobalAdmissionGate(2);
        assertEquals(Result.ACQUIRED, gate.tryAcquire());
        assertEquals(Result.ACQUIRED, gate.tryAcquire());
        assertEquals(Result.LIMIT_REACHED, gate.tryAcquire());
        assertEquals(2, gate.held());
    }

    @Test
    void releaseFreesASlot() {
        GlobalAdmissionGate gate = new GlobalAdmissionGate(1);
        gate.tryAcquire();
        gate.release();
        assertEquals(0, gate.held());
        assertEquals(Result.ACQUIRED, gate.tryAcquire());
    }

    @Test
    void closeBlocksNewAcquiresButStillAllowsRelease() {
        GlobalAdmissionGate gate = new GlobalAdmissionGate(2);
        gate.tryAcquire();
        gate.close();
        gate.close(); // idempotent
        assertTrue(gate.isClosed());
        assertEquals(Result.CLOSED, gate.tryAcquire());
        assertEquals(1, gate.held());
        gate.release();
        assertEquals(0, gate.held());
        assertTrue(gate.isClosed());
        assertEquals(Result.CLOSED, gate.tryAcquire());
    }

    @Test
    void releaseWithoutAcquireIsRejected() {
        GlobalAdmissionGate gate = new GlobalAdmissionGate(1);
        assertThrows(IllegalStateException.class, gate::release);
        assertFalse(gate.isClosed());
    }

    @Test
    void concurrentAcquireNeverExceedsLimit() throws Exception {
        GlobalAdmissionGate gate = new GlobalAdmissionGate(100);
        AtomicInteger acquired = new AtomicInteger();
        Concurrently.run(16, t -> {
            for (int i = 0; i < 1_000; i++) {
                if (gate.tryAcquire() == Result.ACQUIRED) acquired.incrementAndGet();
            }
        });
        assertEquals(100, acquired.get());
        assertEquals(100, gate.held());
    }

    @Test
    void acquireReleaseChurnNeverExceedsLimitAndEndsAtZero() throws Exception {
        GlobalAdmissionGate gate = new GlobalAdmissionGate(4);
        Concurrently.run(8, t -> {
            for (int i = 0; i < 10_000; i++) {
                if (gate.tryAcquire() == Result.ACQUIRED) {
                    assertTrue(gate.held() <= 4, "held exceeded limit");
                    gate.release();
                }
            }
        });
        assertEquals(0, gate.held());
    }
}
