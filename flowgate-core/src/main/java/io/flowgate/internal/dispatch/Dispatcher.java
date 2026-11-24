
package io.flowgate.internal.dispatch;

import io.flowgate.api.TaskStatus;
import io.flowgate.internal.ledger.ConcurrencyLedger;
import io.flowgate.internal.ledger.ExecutionPermit;
import io.flowgate.internal.queue.QueueRegistry;
import io.flowgate.internal.queue.WorkloadQueue;
import io.flowgate.internal.retry.RetryScheduler;
import io.flowgate.internal.task.TaskRecord;
import io.flowgate.internal.time.TimeSource;
import io.flowgate.policy.PriorityAwareAgingPolicy;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Coordinates scheduling, concurrency allocation,
 * queue removal and worker handoff.
 *
 * <p>Designed for a single dispatcher thread.</p>
 *
 * <p>The executor must provide bounded execution capacity
 * and must not use caller-runs rejection handling.</p>
 */
public final class Dispatcher {

    private final ConcurrencyLedger ledger;
    private final DeficitRoundRobinScheduler scheduler;
    private final PriorityAwareAgingPolicy agingPolicy;
    private final Executor executor;
    private final TimeSource time;
    private final RetryScheduler retryScheduler;

    private final WorkloadQueue[] queues;

    /**
     * Original configured scheduling quantum for each queue.
     * Never modified after construction.
     */
    private final int[] baseQuantum;

    /**
     * Dynamically calculated effective quantum.
     * Recalculated before each scheduling decision.
     */
    private final int[] quantum;

    private final int[] waiting;
    private final boolean[] eligible;

    public Dispatcher(
            QueueRegistry registry,
            ConcurrencyLedger ledger,
            DeficitRoundRobinScheduler scheduler,
            Executor executor,
            TimeSource time,
            int[] quantum,
            PriorityAwareAgingPolicy agingPolicy
    ) {
        this(
                registry,
                ledger,
                scheduler,
                executor,
                time,
                quantum,
                agingPolicy,
                null
        );
    }

    /**
     * @param retryScheduler where failed attempts are parked for retry;
     *                       null disables retries for every queue
     */
    public Dispatcher(
            QueueRegistry registry,
            ConcurrencyLedger ledger,
            DeficitRoundRobinScheduler scheduler,
            Executor executor,
            TimeSource time,
            int[] quantum,
            PriorityAwareAgingPolicy agingPolicy,
            RetryScheduler retryScheduler
    ) {

        Objects.requireNonNull(
                registry,
                "registry must not be null"
        );

        this.retryScheduler = retryScheduler;

        this.ledger = Objects.requireNonNull(
                ledger,
                "ledger must not be null"
        );

        this.scheduler = Objects.requireNonNull(
                scheduler,
                "scheduler must not be null"
        );

        this.executor = Objects.requireNonNull(
                executor,
                "executor must not be null"
        );

        this.time = Objects.requireNonNull(
                time,
                "time must not be null"
        );

        this.agingPolicy = Objects.requireNonNull(
                agingPolicy,
                "agingPolicy must not be null"
        );

        this.queues = registry.ordered();

        if (queues.length == 0
                || ledger.queueCount() != queues.length
                || quantum == null
                || quantum.length != queues.length) {

            throw new IllegalArgumentException(
                    "Queue, ledger and quantum configurations must match"
            );
        }

        this.baseQuantum = quantum.clone();
        this.quantum = this.baseQuantum.clone();

        for (int value : this.baseQuantum) {
            if (value <= 0) {
                throw new IllegalArgumentException(
                        "Scheduling quantum must be positive"
                );
            }
        }

        this.waiting = new int[queues.length];
        this.eligible = new boolean[queues.length];
    }

    /**
     * Attempts to dispatch one task.
     *
     * @return true if a task was successfully handed to the executor
     */
    public boolean dispatchOne() {

        refreshSnapshot();
        refreshEffectiveQuantum();

        int selected = scheduler.selectNext(
                eligible,
                waiting,
                quantum
        );

        if (selected < 0) {
            return false;
        }

        ExecutionPermit permit = ledger.tryAcquire(
                selected,
                waiting
        );

        if (permit == null) {
            scheduler.cancelSelection(selected);
            return false;
        }

        WorkloadQueue queue = queues[selected];

        TaskRecord<?> record = queue.poll();

        if (record == null) {
            permit.close();
            scheduler.cancelSelection(selected);
            return false;
        }

        if (record.status() != TaskStatus.QUEUED) {
            permit.close();
            scheduler.cancelSelection(selected);
            return false;
        }

        if (record.isExpired(time.nanoTime())) {
            expire(record);
            permit.close();
            scheduler.cancelSelection(selected);
            return false;
        }

        if (!record.tryTransition(TaskStatus.SCHEDULED)) {
            permit.close();
            scheduler.cancelSelection(selected);
            return false;
        }

        /*
         * Until execute() returns successfully, the dispatcher
         * owns the permit.
         */
        try {

            executor.execute(
                    new TaskExecution(
                            record,
                            permit,
                            time,
                            queue.config().retryPolicy(),
                            retryScheduler
                    )
            );

        } catch (RejectedExecutionException rejection) {

            failDispatch(record, rejection);
            permit.close();
            scheduler.cancelSelection(selected);

            return false;

        } catch (RuntimeException | Error failure) {

            failDispatch(record, failure);
            permit.close();
            scheduler.cancelSelection(selected);

            throw failure;
        }

        /*
         * Successful execute() transfers permit ownership
         * to TaskExecution.
         */
        scheduler.commitDispatch(selected);

        return true;
    }

    /**
     * Refreshes queue waiting counts and concurrency eligibility.
     */
    private void refreshSnapshot() {

        for (int i = 0; i < queues.length; i++) {
            waiting[i] = queues[i].size();
        }

        ledger.snapshotEligibility(eligible, waiting);
    }

    /**
     * Calculates effective quantum using the oldest visible task.
     *
     * The configured base quantum is preserved. Aging increases
     * the effective quantum without permanently changing the base.
     */
    private void refreshEffectiveQuantum() {

        long now = time.nanoTime();

        for (int i = 0; i < queues.length; i++) {

            WorkloadQueue queue = queues[i];

            TaskRecord<?> head = queue.peek();

            if (head == null) {
                quantum[i] = baseQuantum[i];
                continue;
            }

            long headWaitNanos = Math.max(
                    0L,
                    now - head.enqueueNanos()
            );

            quantum[i] = agingPolicy.effectiveQuantum(
                    queue.config().priority(),
                    baseQuantum[i],
                    headWaitNanos
            );
        }
    }

    private void expire(TaskRecord<?> record) {

        if (record.tryTransition(TaskStatus.EXPIRED)) {

            record.fail(
                    new TimeoutException(
                            "Task deadline exceeded before scheduling: "
                                    + record.id()
                    )
            );
        }
    }

    private void failDispatch(
            TaskRecord<?> record,
            Throwable failure
    ) {

        if (record.tryTransition(TaskStatus.FAILED)) {
            record.fail(failure);
        }
    }
}
