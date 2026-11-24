
package io.flowgate.api;

/**
 * Context passed to a running workload task.
 *
 * <p>Provides cooperative cancellation and deadline visibility.
 * Tasks should periodically inspect these signals during execution.</p>
 *
 * <p>FlowGate does not forcibly terminate tasks when their deadlines
 * expire. Deadline enforcement for running tasks is cooperative.</p>
 */
public interface TaskContext {

    /**
     * Returns whether cancellation has been requested.
     */
    boolean isCancellationRequested();

    /**
     * Returns whether this task has a configured deadline.
     */
    boolean hasDeadline();

    /**
     * Returns whether this task's deadline has elapsed.
     *
     * <p>For tasks without a deadline, this always returns false.</p>
     */
    boolean isDeadlineExceeded();

    /**
     * Returns the remaining time before the deadline, in nanoseconds.
     *
     * @return remaining nanoseconds, zero if expired,
     *         or {@link Long#MAX_VALUE} if no deadline exists
     */
    long remainingTimeNanos();

    /**
     * Returns the current execution attempt number.
     */
    int attempt();
}
