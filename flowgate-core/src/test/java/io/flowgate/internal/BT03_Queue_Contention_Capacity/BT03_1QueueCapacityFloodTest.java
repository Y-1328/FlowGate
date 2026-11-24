package io.flowgate.internal.queue;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class BT03_1QueueCapacityFloodTest {
    @Test void floodNeverExceedsLocalCapacityAndOverflowIsRejected() {
        int capacity = 32;
        GlobalAdmissionGate gate = new GlobalAdmissionGate(capacity);
        WorkloadQueue q = BT03TestSupport.queue("flood", 0, capacity, gate);
        int accepted = 0;
        for (int i = 0; i < 10_000; i++) if (BT03TestSupport.submit(q, "flood", i)) accepted++;
        assertEquals(capacity, accepted);
        assertEquals(capacity, q.size());
        assertEquals(capacity, q.snapshot().size());
        assertEquals(capacity, gate.held());
        for (int i = 0; i < capacity; i++) assertNotNull(q.poll());
        assertNull(q.poll());
        assertEquals(0, q.size());
        assertEquals(0, gate.held());
    }
}
