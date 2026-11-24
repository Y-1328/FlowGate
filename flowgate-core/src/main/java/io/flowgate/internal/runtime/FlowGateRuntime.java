
package io.flowgate.internal.runtime;

import io.flowgate.api.Priority;
import io.flowgate.api.Submission;
import io.flowgate.api.WorkloadTask;
import io.flowgate.config.FlowGateConfig;
import io.flowgate.config.QueueConfig;
import io.flowgate.internal.admission.AdmissionController;
import io.flowgate.internal.admission.ReadmissionResult;
import io.flowgate.internal.deadline.DeadlineManager;
import io.flowgate.internal.dispatch.DeficitRoundRobinScheduler;
import io.flowgate.internal.dispatch.DispatchLoop;
import io.flowgate.internal.dispatch.Dispatcher;
import io.flowgate.internal.ledger.ConcurrencyLedger;
import io.flowgate.internal.queue.GlobalAdmissionGate;
import io.flowgate.internal.queue.QueueRegistry;
import io.flowgate.internal.queue.WorkloadQueue;
import io.flowgate.internal.retry.RetryScheduler;
import io.flowgate.internal.task.TaskRecord;
import io.flowgate.internal.time.SystemTimeSource;
import io.flowgate.internal.time.TimeSource;
import io.flowgate.policy.PriorityAwareAgingPolicy;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Internal composition root for a FlowGate runtime.
 *
 * <p>Connects admission, queues, resource allocation,
 * scheduling, dispatch, deadline management, bounded retry and worker
 * execution.</p>
 *
 * <p>Retries (opt-in per queue through {@code QueueConfig.retryPolicy()}) never
 * execute outside normal dispatch: a failed attempt parks the task in a bounded
 * {@link RetryScheduler}, and when its backoff elapses the task is offered back
 * to {@link AdmissionController#readmit(TaskRecord)} under {@code lifecycleLock},
 * exactly like a submission. The scheduler's wake-ups run on the existing
 * single deadline thread; no thread is added.</p>
 *
 * <p>This class is internal infrastructure, not the
 * developer-facing public API.</p>
 */
public final class FlowGateRuntime implements AutoCloseable {

    private static final long DEADLINE_SWEEP_INTERVAL_MILLIS = 10L;

    /** How often cancelled or expired retry-wait entries are discarded. */
    private static final long RETRY_PURGE_INTERVAL_MILLIS = 1_000L;

    /** Marker: bound the retry-wait store by the global admission capacity. */
    private static final int DEFAULT_MAX_PENDING_RETRIES = -1;

    /** Serialises submit(), re-admission and close(). Package-private so tests can hold it. */
    final Object lifecycleLock = new Object();

    private final QueueRegistry registry;
    private final ConcurrencyLedger ledger;
    private final AdmissionController admission;
    private final ThreadPoolExecutor workers;
    private final DispatchLoop dispatchLoop;
    private final RetryScheduler retryScheduler;

    private final DeadlineManager deadlineManager;
    private final ScheduledExecutorService deadlineSweeper;

    /** The single scheduler thread; lets close() avoid waiting for itself. */
    private volatile Thread timerThread;

    private final ThreadLocal<Boolean> workerContext =
            ThreadLocal.withInitial(() -> false);

    private final CountDownLatch shutdownInitiated =
            new CountDownLatch(1);

    private boolean closeInitiated;
    private boolean started;

    /**
     * Creates a FlowGate runtime using the system monotonic clock.
     */
    public FlowGateRuntime(
            FlowGateConfig config,
            Duration idleWait
    ) {
        this(config, idleWait, new SystemTimeSource());
    }

    /**
     * Package-private constructor allowing an injectable time source.
     */
    FlowGateRuntime(
            FlowGateConfig config,
            Duration idleWait,
            TimeSource time
    ) {
        this(config, idleWait, time, DEFAULT_MAX_PENDING_RETRIES);
    }

    /**
     * Package-private constructor that also bounds the retry-wait store.
     *
     * @param maxPendingRetries maximum tasks waiting for a retry at once, or a
     *                          negative value for the default (the sum of all
     *                          queue capacities)
     */
    FlowGateRuntime(
            FlowGateConfig config,
            Duration idleWait,
            TimeSource time,
            int maxPendingRetries
    ) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(idleWait, "idleWait");
        Objects.requireNonNull(time, "time");

        validateQueueIds(config.queues());

        int globalQueueCapacity =
                calculateGlobalQueueCapacity(config.queues());

        GlobalAdmissionGate gate =
                new GlobalAdmissionGate(globalQueueCapacity);

        this.registry = new QueueRegistry(
                config.queues(),
                gate
        );

        this.admission = new AdmissionController(
                registry,
                gate,
                time
        );

        this.deadlineManager = new DeadlineManager(
                registry,
                time
        );

        this.ledger = new ConcurrencyLedger(
                config.globalConcurrency(),
                registry
        );

        this.workers = createWorkerPool(
                config.globalConcurrency()
        );

        // The single scheduler thread serves both deadline sweeps and retry
        // wake-ups. Cancelled timers are removed so at most one retry timer
        // is ever outstanding.
        ThreadFactory timerThreadFactory = runnable -> {
            Thread thread = new Thread(
                    runnable,
                    "flowgate-deadline-sweeper"
            );

            thread.setDaemon(true);
            timerThread = thread;
            return thread;
        };

        ScheduledThreadPoolExecutor timer =
                new ScheduledThreadPoolExecutor(1, timerThreadFactory);

        timer.setRemoveOnCancelPolicy(true);

        this.deadlineSweeper = timer;

        this.retryScheduler = new RetryScheduler(
                time,
                maxPendingRetries < 0 ? globalQueueCapacity : maxPendingRetries,
                this::readmit,
                (task, delayNanos) ->
                        timer.schedule(task, delayNanos, TimeUnit.NANOSECONDS)
        );

        int[] quantum = config.queues()
                .stream()
                .mapToInt(QueueConfig::weight)
                .toArray();

        /*
         * Priority-specific aging configuration.
         *
         * Aging interval: 5 seconds.
         * Maximum boosts:
         * CRITICAL = 0
         * HIGH     = 2
         * NORMAL   = 4
         * LOW      = 6
         */
        PriorityAwareAgingPolicy agingPolicy =
                new PriorityAwareAgingPolicy(
                        Duration.ofSeconds(5).toNanos(),
                        4,
                        Map.of(
                                Priority.CRITICAL, 0,
                                Priority.HIGH, 2,
                                Priority.NORMAL, 4,
                                Priority.LOW, 6
                        )
                );

        Dispatcher dispatcher = new Dispatcher(
                registry,
                ledger,
                new DeficitRoundRobinScheduler(registry.size()),
                workers,
                time,
                quantum,
                agingPolicy,
                retryScheduler
        );

        this.dispatchLoop = new DispatchLoop(
                dispatcher,
                idleWait
        );
    }

    /**
     * Starts the dispatcher and deadline sweeper exactly once.
     *
     * <p>If startup fails at any point, the runtime is permanently
     * closed and all partially initialized resources are cleaned up.</p>
     */
    public void start() {

        synchronized (lifecycleLock) {

            ensureOpen();

            if (started) {
                throw new IllegalStateException(
                        "FlowGate runtime has already been started"
                );
            }

            try {

                // Schedule periodic deadline sweeps.
                deadlineSweeper.scheduleWithFixedDelay(
                        deadlineManager::sweep,
                        DEADLINE_SWEEP_INTERVAL_MILLIS,
                        DEADLINE_SWEEP_INTERVAL_MILLIS,
                        TimeUnit.MILLISECONDS
                );

                // Discard retry-wait entries of cancelled tasks.
                deadlineSweeper.scheduleWithFixedDelay(
                        retryScheduler::purgeDead,
                        RETRY_PURGE_INTERVAL_MILLIS,
                        RETRY_PURGE_INTERVAL_MILLIS,
                        TimeUnit.MILLISECONDS
                );

                // Start the dispatcher only after scheduling succeeds.
                dispatchLoop.start();

                started = true;

            } catch (RuntimeException | Error failure) {

                // Startup failure is terminal for this runtime.
                closeInitiated = true;

                try {
                    admission.shutdown();
                } catch (Throwable cleanupFailure) {
                    addSuppressedSafely(failure, cleanupFailure);
                }

                try {
                    initiateShutdown();
                } catch (Throwable cleanupFailure) {
                    addSuppressedSafely(failure, cleanupFailure);
                }

                throw failure;
            }
        }
    }

    /**
     * Submits a task using the queue's configured default deadline.
     *
     * <p>If the queue has no default deadline, the task has no deadline.</p>
     */
    public <T> Submission<T> submit(
            String queueId,
            WorkloadTask<T> task
    ) {

        synchronized (lifecycleLock) {

            ensureOpen();

            QueueConfig queueConfig =
                    registry.require(queueId).config();

            Duration defaultDeadline =
                    queueConfig.defaultDeadline();

            Submission<T> submission =
                    admission.submit(
                            queueId,
                            task,
                            defaultDeadline
                    );

            signalIfAccepted(submission);

            return submission;
        }
    }

    /**
     * Submits a task with an explicitly supplied deadline.
     *
     * <p>A null deadline means no deadline, even if the queue
     * has a configured default deadline.</p>
     */
    public <T> Submission<T> submit(
            String queueId,
            WorkloadTask<T> task,
            Duration deadline
    ) {

        synchronized (lifecycleLock) {

            ensureOpen();

            Submission<T> submission =
                    admission.submit(
                            queueId,
                            task,
                            deadline
                    );

            signalIfAccepted(submission);

            return submission;
        }
    }

    /**
     * Re-admits a retrying task. Runs under the same lock as submit() and
     * close(), so a re-admission is either completed before shutdown begins
     * (and the task is then cancelled with the other waiting tasks) or it
     * observes shutdown and is refused. It can never publish a task after the
     * queues have been drained.
     */
    ReadmissionResult readmit(TaskRecord<?> record) {

        ReadmissionResult result;

        synchronized (lifecycleLock) {
            result = closeInitiated
                    ? ReadmissionResult.SHUTDOWN
                    : admission.readmit(record);
        }

        if (result == ReadmissionResult.ADMITTED) {
            dispatchLoop.signal();
        }

        return result;
    }

    /** Active executions holding a permit. Diagnostic and test use only. */
    int activeExecutions() {
        return ledger.activeTotal();
    }

    /** Tasks holding queue admission capacity. Diagnostic and test use only. */
    int queuedTasks() {

        int total = 0;

        for (WorkloadQueue queue : registry.ordered()) {
            total += queue.size();
        }

        return total;
    }

    /** Tasks waiting for a retry backoff. Diagnostic and test use only. */
    int pendingRetries() {
        return retryScheduler.pending();
    }

    private void signalIfAccepted(Submission<?> submission) {

        if (submission.isAccepted()) {
            dispatchLoop.signal();
        }
    }

    public boolean isRunning() {
        return dispatchLoop.isRunning();
    }

    /**
     * Stops admission and dispatch, stops deadline management,
     * cancels waiting tasks, and gracefully shuts down workers.
     *
     * <p>Already-dispatched tasks are allowed to finish.
     * Calls from worker threads initiate shutdown without
     * waiting for their own termination.</p>
     */
    @Override
    public void close() {

        boolean shutdownOwner = false;

        synchronized (lifecycleLock) {

            if (!closeInitiated) {

                closeInitiated = true;

                admission.shutdown();

                shutdownOwner = true;
            }
        }

        if (shutdownOwner) {
            initiateShutdown();
        } else {
            // The shutdown owner may be waiting for the scheduler thread
            // to terminate. A callback on that same thread must not wait
            // for shutdown initiation, or both threads will deadlock.
            if (Thread.currentThread() == timerThread) {
                return;
            }

            awaitShutdownInitiation();
        }

        // A worker must never wait for its own termination.
        if (Boolean.TRUE.equals(workerContext.get())) {
            return;
        }

        awaitWorkerTermination();
    }

    /**
     * Performs shutdown in a strict order:
     *
     * 1. Stop the scheduler thread (deadline sweeps and retry wake-ups)
     *    and wait for any callback running on it to finish.
     * 2. Close the retry scheduler: tasks waiting for a retry are cancelled
     *    and later retry requests are refused.
     * 3. Stop the dispatcher.
     * 4. Cancel remaining queued tasks.
     * 5. Shut down workers.
     */
    private void initiateShutdown() {

        try {

            stopDeadlineSweeper();

        } finally {

            try {

                retryScheduler.close();

            } finally {

                try {

                    dispatchLoop.close();

                } finally {

                    try {

                        cancelWaitingTasks();

                    } finally {

                        try {

                            workers.shutdown();

                        } finally {

                            shutdownInitiated.countDown();
                        }
                    }
                }
            }
        }
    }

    /**
     * Stops the scheduled deadline sweeper and waits for termination.
     */
    private void stopDeadlineSweeper() {

        deadlineSweeper.shutdownNow();

        // A future callback running on the scheduler thread may close the
        // runtime; that thread must not wait for its own termination.
        if (Thread.currentThread() == timerThread) {
            return;
        }

        boolean interrupted = false;

        try {

            while (!deadlineSweeper.isTerminated()) {

                try {

                    deadlineSweeper.awaitTermination(
                            Long.MAX_VALUE,
                            TimeUnit.NANOSECONDS
                    );

                } catch (InterruptedException exception) {

                    interrupted = true;
                }
            }

        } finally {

            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Cancels every task that remains in a workload queue.
     */
    private void cancelWaitingTasks() {

        for (WorkloadQueue queue : registry.ordered()) {

            TaskRecord<?> record;

            while ((record = queue.poll()) != null) {
                record.cancel();
            }
        }
    }

    private void awaitShutdownInitiation() {

        boolean interrupted = false;

        try {

            for (;;) {

                try {

                    shutdownInitiated.await();
                    return;

                } catch (InterruptedException exception) {

                    interrupted = true;
                }
            }

        } finally {

            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void awaitWorkerTermination() {

        boolean interrupted = false;

        try {

            while (!workers.isTerminated()) {

                try {

                    workers.awaitTermination(
                            Long.MAX_VALUE,
                            TimeUnit.NANOSECONDS
                    );

                } catch (InterruptedException exception) {

                    interrupted = true;
                }
            }

        } finally {

            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void ensureOpen() {

        if (closeInitiated) {

            throw new IllegalStateException(
                    "FlowGate runtime is closed"
            );
        }
    }

    /**
     * Adds a cleanup failure without replacing the original failure.
     */
    private static void addSuppressedSafely(
            Throwable original,
            Throwable cleanupFailure
    ) {
        if (original != cleanupFailure) {
            original.addSuppressed(cleanupFailure);
        }
    }

    private ThreadPoolExecutor createWorkerPool(int concurrency) {

        AtomicInteger threadIds = new AtomicInteger();

        ThreadFactory factory = runnable -> new Thread(
                () -> {

                    workerContext.set(true);

                    try {

                        runnable.run();

                    } finally {

                        workerContext.remove();
                    }
                },
                "flowgate-worker-" + threadIds.incrementAndGet()
        );

        return new ThreadPoolExecutor(
                concurrency,
                concurrency,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(concurrency),
                factory,
                new ThreadPoolExecutor.AbortPolicy()
        );
    }

    private static int calculateGlobalQueueCapacity(
            java.util.List<QueueConfig> queues
    ) {

        long total = 0;

        for (QueueConfig queue : queues) {

            total += queue.capacity();

            if (total > Integer.MAX_VALUE) {

                throw new IllegalArgumentException(
                        "Total queue capacity exceeds supported range"
                );
            }
        }

        return (int) total;
    }

    private static void validateQueueIds(
            java.util.List<QueueConfig> queues
    ) {

        Set<String> ids = new HashSet<>();

        for (QueueConfig queue : queues) {

            Objects.requireNonNull(
                    queue,
                    "queue configuration"
            );

            if (!ids.add(queue.id())) {

                throw new IllegalArgumentException(
                        "Duplicate queue ID: " + queue.id()
                );
            }
        }
    }
}
