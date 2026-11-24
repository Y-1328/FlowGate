package io.flowgate.testsupport;

import io.flowgate.internal.time.TimeSource;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/** A monotonic clock that only moves when a test moves it. Safe to read from many threads. */
public final class ManualTimeSource implements TimeSource {

    private final AtomicLong now;

    public ManualTimeSource(long startNanos) {
        this.now = new AtomicLong(startNanos);
    }

    public ManualTimeSource() {
        this(1_000L);
    }

    @Override
    public long nanoTime() {
        return now.get();
    }

    /** @return the new time */
    public long advance(long nanos) {
        return now.addAndGet(nanos);
    }

    public long advance(Duration duration) {
        return advance(duration.toNanos());
    }
}
