package io.flowgate.internal.queue;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Global bound on queued tasks AND the shutdown switch, in one atomic word:
 * bit 31 = closed, bits 0-30 = tasks currently held (reserved or queued).
 *
 * <p>Folding "closed" into the counter makes shutdown linearizable with admission: a successful
 * {@link #tryAcquire()} happened strictly before {@link #close()} took effect, so once close()
 * returns no new task can ever be admitted. No separate in-flight counter or lock is needed.
 */
public final class GlobalAdmissionGate {
    public enum Result { ACQUIRED, LIMIT_REACHED, CLOSED }

    private static final int CLOSED_BIT = 0x80000000;
    private static final int COUNT_MASK = 0x7FFFFFFF;

    private final int limit;
    private final AtomicInteger state = new AtomicInteger();

    public GlobalAdmissionGate(int limit) {
        if (limit < 1) throw new IllegalArgumentException("limit must be >= 1");
        this.limit = limit;
    }

    public Result tryAcquire() {
        for (;;) {
            int s = state.get();
            if (s < 0) return Result.CLOSED;
            if (s >= limit) return Result.LIMIT_REACHED;
            if (state.compareAndSet(s, s + 1)) return Result.ACQUIRED;
        }
    }

    /** @throws IllegalStateException on release without a matching acquire (accounting bug) */
    public void release() {
        for (;;) {
            int s = state.get();
            if ((s & COUNT_MASK) == 0) throw new IllegalStateException("release without acquire");
            if (state.compareAndSet(s, s - 1)) return;
        }
    }

    /** Idempotent. After this returns, tryAcquire never returns ACQUIRED again. */
    public void close() {
        state.getAndUpdate(s -> s | CLOSED_BIT);
    }

    public boolean isClosed() { return state.get() < 0; }

    public int held() { return state.get() & COUNT_MASK; }
}
