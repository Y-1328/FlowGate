
package io.flowgate.internal.deadline;

import io.flowgate.api.TaskStatus;
import io.flowgate.internal.queue.QueueRegistry;
import io.flowgate.internal.queue.WorkloadQueue;
import io.flowgate.internal.task.TaskRecord;
import io.flowgate.internal.time.TimeSource;

import java.util.Objects;
import java.util.concurrent.TimeoutException;

/**
 * Detects and expires tasks whose deadlines have elapsed.
 *
 * <p>The manager scans snapshots of configured workload queues.
 * It only expires tasks that are still in the QUEUED state.</p>
 *
 * <p>Queue capacity is released through WorkloadQueue.remove().
 * If the dispatcher has already polled a task, the manager does
 * not release its reservation a second time.</p>
 *
 * <p>This class does not own a background thread. Its sweep()
 * method can be invoked periodically by a runtime scheduler or
 * directly by deterministic tests.</p>
 */
public final class DeadlineManager {

    private final QueueRegistry registry;
    private final TimeSource time;

    public DeadlineManager(
            QueueRegistry registry,
            TimeSource time
    ) {
        this.registry = Objects.requireNonNull(
                registry,
                "registry"
        );

        this.time = Objects.requireNonNull(
                time,
                "time"
        );
    }

    /**
     * Scans all workload queues and expires overdue queued tasks.
     *
     * @return number of tasks successfully transitioned to EXPIRED
     */
    public int sweep() {

        int expiredCount = 0;
        long now = time.nanoTime();

        for (WorkloadQueue queue : registry.ordered()) {

            for (TaskRecord<?> record : queue.snapshot()) {

                if (record.status() != TaskStatus.QUEUED) {
                    continue;
                }

                if (!record.isExpired(now)) {
                    continue;
                }

                // Only one competing lifecycle transition can win.
                if (!record.tryTransition(TaskStatus.EXPIRED)) {
                    continue;
                }

                // Complete the task exceptionally.
                record.fail(
                        new TimeoutException(
                                "Task deadline exceeded: " + record.id()
                        )
                );

                // Release admission capacity only if still queued.
                queue.remove(record);

                expiredCount++;
            }
        }

        return expiredCount;
    }
}
