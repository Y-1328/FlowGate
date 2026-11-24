package io.flowgate.internal.queue;

import io.flowgate.config.QueueConfig;
import io.flowgate.exception.UnknownQueueException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Built once, never mutated; safe to share through final fields. */
public final class QueueRegistry {
    private final WorkloadQueue[] ordered;
    private final Map<String, WorkloadQueue> byId;

    public QueueRegistry(Collection<QueueConfig> configsInOrder, GlobalAdmissionGate gate) {
        WorkloadQueue[] arr = new WorkloadQueue[configsInOrder.size()];
        Map<String, WorkloadQueue> map = new HashMap<>(Math.max(16, (int) (arr.length / 0.75f) + 1));
        int i = 0;
        for (QueueConfig c : configsInOrder) {
            WorkloadQueue q = new WorkloadQueue(i, c, gate);
            arr[i++] = q;
            map.put(c.id(), q);
        }
        this.ordered = arr;
        this.byId = Collections.unmodifiableMap(map);
    }

    public WorkloadQueue require(String id) {
        WorkloadQueue q = byId.get(id);
        if (q == null) throw new UnknownQueueException(id);
        return q;
    }

    /** Configured order; callers must not modify the array. */
    public WorkloadQueue[] ordered() { return ordered.clone(); }

    public int size() { return ordered.length; }
}
