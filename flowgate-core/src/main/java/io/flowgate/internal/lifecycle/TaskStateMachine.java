
package io.flowgate.internal.lifecycle;

import io.flowgate.api.TaskStatus;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Controls legal task lifecycle transitions.
 *
 * Thread-safe. Only one concurrent transition from a given
 * state can succeed.
 */
public final class TaskStateMachine {

    private final AtomicReference<TaskStatus> status =
            new AtomicReference<>(TaskStatus.CREATED);

    public TaskStatus status() {
        return status.get();
    }

    public boolean tryTransition(TaskStatus target) {
        Objects.requireNonNull(target, "target must not be null");

        while (true) {
            TaskStatus current = status.get();

            if (!isAllowed(current, target)) {
                return false;
            }

            if (status.compareAndSet(current, target)) {
                return true;
            }
        }
    }

    private static boolean isAllowed(TaskStatus from, TaskStatus to) {
        return switch (from) {

            case CREATED ->
                    to == TaskStatus.QUEUED
                            || to == TaskStatus.REJECTED;

            case QUEUED ->
                    to == TaskStatus.SCHEDULED
                            || to == TaskStatus.CANCELLED
                            || to == TaskStatus.EXPIRED;

            case SCHEDULED ->
                    to == TaskStatus.RUNNING
                            || to == TaskStatus.FAILED
                            || to == TaskStatus.CANCELLED
                            || to == TaskStatus.EXPIRED;

            case RUNNING ->
                    to == TaskStatus.COMPLETED
                            || to == TaskStatus.FAILED
                            || to == TaskStatus.CANCELLED
                            || to == TaskStatus.RETRY_WAIT;

            case RETRY_WAIT ->
                    to == TaskStatus.QUEUED
                            || to == TaskStatus.FAILED
                            || to == TaskStatus.CANCELLED
                            || to == TaskStatus.EXPIRED;

            case COMPLETED, FAILED, CANCELLED, EXPIRED, REJECTED ->
                    false;
        };
    }
}
