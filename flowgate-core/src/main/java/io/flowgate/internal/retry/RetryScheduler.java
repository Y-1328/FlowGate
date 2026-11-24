package io.flowgate.internal.retry;

import io.flowgate.api.TaskStatus;
import io.flowgate.exception.RetryFailedException;
import io.flowgate.internal.admission.ReadmissionResult;
import io.flowgate.internal.task.TaskRecord;
import io.flowgate.internal.time.TimeSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Holds tasks in RETRY_WAIT until their backoff elapses, then offers them back
 * to <em>normal</em> admission.
 *
 * <h2>Why this component exists</h2>
 * A task that is waiting to be retried holds no queue or global admission
 * capacity (capacity is released when a task leaves its queue), so admission
 * limits do not bound how many tasks may be waiting for a retry. This class is
 * that bound.
 *
 * <h2>Resource bounds</h2>
 * <ul>
 *   <li><b>Entries:</b> at most {@code maxPending}. A request beyond the bound is
 *       refused immediately ({@link ScheduleResult#CAPACITY_FULL}); nothing is
 *       parked. Entries of tasks that are no longer in RETRY_WAIT (cancelled,
 *       expired) are purged before a request is refused, so dead entries never
 *       consume the bound.</li>
 *   <li><b>Threads:</b> none. The owner supplies a {@link Timer}; the runtime
 *       backs it with its existing single scheduler thread.</li>
 *   <li><b>Timers:</b> at most one outstanding at any moment. When an earlier
 *       entry arrives the previous timer is cancelled and replaced.</li>
 *   <li><b>Per-task memory:</b> one entry. Failure history is not accumulated.</li>
 * </ul>
 *
 * <h2>Concurrency</h2>
 * One lock protects the entry store, the closed flag and the armed timer. It is
 * held only for short, non-blocking operations: never while calling the
 * readmitter, completing a future, or running any user code. Each entry is
 * removed from the store exactly once before it is processed, so a task can
 * never be re-admitted twice for one backoff.
 *
 * <h2>Deadlines</h2>
 * An entry's due time is {@code min(now + backoff, deadline)}. If the deadline is
 * reached first, processing finds the task expired and moves it to EXPIRED, so a
 * task expires at its deadline rather than lingering until the backoff ends.
 */
public final class RetryScheduler implements AutoCloseable {

    /** Result of {@link #schedule(TaskRecord, long)}. */
    public enum ScheduleResult {
        /** The task is parked and will be offered to admission when due. */
        SCHEDULED,
        /** The bounded retry-wait capacity is full; the task was not parked. */
        CAPACITY_FULL,
        /** The scheduler is closed (or its timer is gone); the task was not parked. */
        CLOSED
    }

    /** Arms a one-shot wake-up. May throw {@link RejectedExecutionException} during shutdown. */
    @FunctionalInterface
    public interface Timer {
        Future<?> schedule(Runnable task, long delayNanos);
    }

    /** Offers a task back to normal admission. Must not hold locks the scheduler needs. */
    @FunctionalInterface
    public interface Readmitter {
        ReadmissionResult readmit(TaskRecord<?> record);
    }

    private static final long MAX_DELAY_NANOS = Long.MAX_VALUE / 4;

    private static final class Entry {
        final TaskRecord<?> record;
        final long dueNanos;
        final long sequence;

        Entry(TaskRecord<?> record, long dueNanos, long sequence) {
            this.record = record;
            this.dueNanos = dueNanos;
            this.sequence = sequence;
        }
    }

    private final TimeSource time;
    private final int maxPending;
    private final Readmitter readmitter;
    private final Timer timer;

    private final ReentrantLock lock = new ReentrantLock();

    private final PriorityQueue<Entry> entries = new PriorityQueue<>(
            (a, b) -> {
                long difference = a.dueNanos - b.dueNanos; // overflow-safe ordering
                return difference != 0
                        ? (difference < 0 ? -1 : 1)
                        : Long.compare(a.sequence, b.sequence);
            }
    );

    private long sequence;
    private boolean closed;
    private Future<?> armed;
    private long armedDueNanos;

    public RetryScheduler(
            TimeSource time,
            int maxPending,
            Readmitter readmitter,
            Timer timer
    ) {
        this.time = Objects.requireNonNull(time, "time");
        this.readmitter = Objects.requireNonNull(readmitter, "readmitter");
        this.timer = Objects.requireNonNull(timer, "timer");

        if (maxPending < 1) {
            throw new IllegalArgumentException("maxPending must be >= 1");
        }

        this.maxPending = maxPending;
    }

    /**
     * Parks a task that is already in RETRY_WAIT until {@code delayNanos} elapse
     * (or its deadline, if sooner).
     *
     * <p>Never blocks and never sleeps. The caller keeps responsibility for the
     * task when the result is not {@link ScheduleResult#SCHEDULED}.
     */
    public ScheduleResult schedule(TaskRecord<?> record, long delayNanos) {

        Objects.requireNonNull(record, "record");

        if (delayNanos < 0) {
            throw new IllegalArgumentException("delayNanos must be >= 0");
        }

        lock.lock();

        try {
            if (closed) {
                return ScheduleResult.CLOSED;
            }

            long now = time.nanoTime();

            if (entries.size() >= maxPending) {
                purgeDeadEntriesLocked();

                if (entries.size() >= maxPending) {
                    return ScheduleResult.CAPACITY_FULL;
                }
            }

            long delay = Math.min(
                    Math.min(delayNanos, MAX_DELAY_NANOS),
                    record.remainingTimeNanos(now)
            );

            Entry entry = new Entry(record, now + delay, sequence++);
            entries.add(entry);

            try {
                armLocked(now);
            } catch (RejectedExecutionException timerGone) {
                entries.remove(entry);
                return ScheduleResult.CLOSED;
            } catch (RuntimeException unexpected) {
                // Never leave an entry behind for a task the caller will fail.
                entries.remove(entry);
                throw unexpected;
            }

            return ScheduleResult.SCHEDULED;

        } finally {
            lock.unlock();
        }
    }

    /**
     * Processes every entry that is due. Invoked by the timer; also callable
     * directly so tests can drive it with a manual clock.
     *
     * @return the number of entries processed
     */
    public int runDue() {

        List<Entry> due = new ArrayList<>();

        lock.lock();

        try {
            if (closed) {
                return 0;
            }

            long now = time.nanoTime();

            if (armed != null && armedDueNanos - now <= 0) {
                armed = null; // the timer that was armed for this instant has fired
            }

            Entry head;

            while ((head = entries.peek()) != null && head.dueNanos - now <= 0) {
                due.add(entries.poll());
            }

        } finally {
            lock.unlock();
        }

        // Outside the lock: re-admission takes runtime locks and completes
        // futures, which can run arbitrary user callbacks.
        Error fatal = null;

        for (Entry entry : due) {
            try {
                process(entry.record);
            } catch (Throwable failure) {
                abandon(entry.record, failure);

                if (failure instanceof Error error && fatal == null) {
                    fatal = error;
                }
            }
        }

        lock.lock();

        try {
            if (!closed) {
                armLocked(time.nanoTime());
            }
        } catch (RejectedExecutionException timerGone) {
            // Shutting down: close() will drain what is left.
        } finally {
            lock.unlock();
        }

        if (fatal != null) {
            throw fatal;
        }

        return due.size();
    }

    private void process(TaskRecord<?> record) {

        // Cancelled, expired or otherwise finished while waiting: drop silently.
        if (record.status() != TaskStatus.RETRY_WAIT) {
            return;
        }

        ReadmissionResult result = readmitter.readmit(record);

        switch (result) {
            case ADMITTED, NOT_WAITING -> {
                // Nothing further to do.
            }
            case DEADLINE_EXPIRED -> record.expireDuringRetry();
            case QUEUE_FULL, GLOBAL_LIMIT -> record.failRetry(
                    new RetryFailedException(
                            RetryFailedException.Reason.READMISSION_REJECTED,
                            record.attempt(),
                            "Task " + record.id() + " could not be re-admitted for a retry ("
                                    + result + ") after " + record.attempt()
                                    + " attempt(s)",
                            record.lastFailure()
                    )
            );
            case SHUTDOWN -> record.cancel();
        }
    }

    /** Last resort: an unexpected exception must never strand a task in RETRY_WAIT. */
    private static void abandon(TaskRecord<?> record, Throwable failure) {
        Throwable last = record.lastFailure();

        RetryFailedException wrapper = new RetryFailedException(
                RetryFailedException.Reason.SCHEDULING_FAILED,
                record.attempt(),
                "Task " + record.id() + " could not be re-admitted for a retry: " + failure,
                last != null ? last : failure
        );

        if (last != null && last != failure) {
            wrapper.addSuppressed(failure);
        }

        record.failRetry(wrapper);
    }

    /**
     * Removes entries whose task is no longer waiting for a retry (cancelled
     * or expired) so they do not hold memory or capacity until their backoff
     * would have ended. Linear in the number of parked tasks; the runtime calls
     * it about once per second.
     *
     * @return the number of entries removed
     */
    public int purgeDead() {
        lock.lock();

        try {
            int before = entries.size();
            purgeDeadEntriesLocked();
            return before - entries.size();
        } finally {
            lock.unlock();
        }
    }

    /** Number of tasks currently parked. */
    public int pending() {
        lock.lock();

        try {
            return entries.size();
        } finally {
            lock.unlock();
        }
    }

    public int capacity() {
        return maxPending;
    }

    public boolean isClosed() {
        lock.lock();

        try {
            return closed;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Stops accepting work, drops the timer and cancels every parked task.
     *
     * <p>Idempotent. After it returns, {@link #schedule} answers
     * {@link ScheduleResult#CLOSED} and no further re-admission is started.
     * Cancelling a task completes its future, so it is done outside the lock.
     */
    @Override
    public void close() {

        List<Entry> drained;

        lock.lock();

        try {
            if (closed) {
                return;
            }

            closed = true;

            if (armed != null) {
                armed.cancel(false);
                armed = null;
            }

            drained = new ArrayList<>(entries);
            entries.clear();

        } finally {
            lock.unlock();
        }

        for (Entry entry : drained) {
            entry.record.cancel();
        }
    }

    // ---- internals (lock held) -------------------------------------------

    private void armLocked(long now) {

        Entry head = entries.peek();

        if (head == null) {
            if (armed != null) {
                armed.cancel(false);
                armed = null;
            }
            return;
        }

        // An outstanding timer that fires no later than the head is enough.
        if (armed != null && !armed.isDone() && head.dueNanos - armedDueNanos >= 0) {
            return;
        }

        if (armed != null) {
            armed.cancel(false);
            armed = null;
        }

        long delay = Math.max(0L, head.dueNanos - now);

        armed = Objects.requireNonNull(
                timer.schedule(this::onTimer, delay),
                "timer must return a handle"
        );
        armedDueNanos = head.dueNanos;
    }

    private void purgeDeadEntriesLocked() {
        entries.removeIf(entry -> entry.record.status() != TaskStatus.RETRY_WAIT);
    }

    private void onTimer() {
        runDue();
    }
}
