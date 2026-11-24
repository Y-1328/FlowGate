package io.flowgate.testsupport;

import io.flowgate.internal.retry.RetryScheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

/**
 * A {@link RetryScheduler.Timer} that never fires by itself, so tests decide
 * exactly when time passes (by moving the clock and calling runDue()).
 * Records every arming request and honours cancellation.
 */
public final class FakeRetryTimer implements RetryScheduler.Timer {

    public static final class Armed {
        public final long delayNanos;
        public final CompletableFuture<Void> handle = new CompletableFuture<>();

        Armed(long delayNanos) {
            this.delayNanos = delayNanos;
        }

        public boolean cancelled() {
            return handle.isCancelled();
        }
    }

    private final List<Armed> armed = Collections.synchronizedList(new ArrayList<>());
    private volatile boolean rejecting;
    private volatile RuntimeException failure;

    @Override
    public Future<?> schedule(Runnable task, long delayNanos) {
        if (rejecting) {
            throw new RejectedExecutionException("timer shut down");
        }

        if (failure != null) {
            throw failure;
        }

        Armed entry = new Armed(delayNanos);
        armed.add(entry);
        return entry.handle;
    }

    /** Timers that were armed and neither cancelled nor completed. */
    public int outstanding() {
        synchronized (armed) {
            return (int) armed.stream().filter(a -> !a.handle.isDone()).count();
        }
    }

    public int totalArmed() {
        return armed.size();
    }

    public Armed last() {
        return armed.get(armed.size() - 1);
    }

    public List<Armed> all() {
        synchronized (armed) {
            return new ArrayList<>(armed);
        }
    }

    public void reject() {
        rejecting = true;
    }

    public void failWith(RuntimeException failure) {
        this.failure = failure;
    }
}
