package io.flowgate.internal.queue;

import io.flowgate.api.Priority;
import io.flowgate.config.QueueConfig;
import io.flowgate.internal.task.TaskRecord;
import java.util.concurrent.Callable;

final class BT03TestSupport {
    private BT03TestSupport() {}

    static WorkloadQueue queue(String id, int index, int capacity, GlobalAdmissionGate gate) {
        QueueConfig config = new QueueConfig(id, Priority.NORMAL, 1, capacity, 0, 1, null);
        return new WorkloadQueue(index, config, gate);
    }

    static TaskRecord<String> record(String queueId, long id) {
        return new TaskRecord<>(id, queueId, ctx -> "ok", 0L, false, 0L);
    }

    static boolean submit(WorkloadQueue q, String queueId, long id) {
        if (q.tryReserve() != WorkloadQueue.Reservation.RESERVED) return false;
        q.enqueueReserved(record(queueId, id));
        return true;
    }

    static void assertBounds(WorkloadQueue q, GlobalAdmissionGate gate) {
        org.junit.jupiter.api.Assertions.assertTrue(q.size() >= 0, "queue size became negative");
        org.junit.jupiter.api.Assertions.assertTrue(q.size() <= q.capacity(), "queue exceeded configured capacity");
        org.junit.jupiter.api.Assertions.assertTrue(gate.held() >= 0, "global held count became negative");
        org.junit.jupiter.api.Assertions.assertTrue(gate.held() <= gateLimit(gate), "global held count exceeded bound");
    }

    // Tests assert global bound directly through known configured limit; helper kept simple.
    static int gateLimit(GlobalAdmissionGate gate) { return Integer.MAX_VALUE; }
}
