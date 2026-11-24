
package io.flowgate.api;

import java.util.concurrent.CompletableFuture;

/**
 * Public handle for observing and cancelling an accepted task.
 */
public interface TaskHandle<T> {

    long id();

    String queueId();

    TaskStatus status();

    /**
     * Attempts to cancel the task.
     *
     * @return true if cancellation successfully changed the task state
     */
    boolean cancel();

    /**
     * Returns a future that completes when the task reaches a terminal state.
     * Callers cannot complete the framework's internal future directly.
     */
    CompletableFuture<T> future();
}
