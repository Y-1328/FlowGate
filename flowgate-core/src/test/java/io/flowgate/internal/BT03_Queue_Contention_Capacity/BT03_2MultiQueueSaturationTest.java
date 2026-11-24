package io.flowgate.internal.queue;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class BT03_2MultiQueueSaturationTest {
    @Test void saturatingQueuesDoesNotCrossLocalOrGlobalBounds() {
        int count = 5, each = 20, globalLimit = 70;
        GlobalAdmissionGate gate = new GlobalAdmissionGate(globalLimit);
        WorkloadQueue[] queues = new WorkloadQueue[count];
        for (int i = 0; i < count; i++) queues[i] = BT03TestSupport.queue("q" + i, i, each, gate);
        int accepted = 0;
        for (int round = 0; round < each + 10; round++)
            for (int i = 0; i < count; i++) if (BT03TestSupport.submit(queues[i], "q" + i, accepted++)) accepted++;
        int heldLocal = 0;
        for (WorkloadQueue q : queues) { assertTrue(q.size() <= each); heldLocal += q.size(); }
        assertTrue(heldLocal <= globalLimit);
        assertEquals(heldLocal, gate.held());
        for (WorkloadQueue q : queues) while (q.poll() != null) { }
        assertEquals(0, gate.held());
    }
}
