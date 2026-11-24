
package io.flowgate.internal.dispatch;

import io.flowgate.api.Priority;
import io.flowgate.api.Submission;
import io.flowgate.api.TaskHandle;
import io.flowgate.api.TaskStatus;
import io.flowgate.config.QueueConfig;
import io.flowgate.internal.admission.AdmissionController;
import io.flowgate.internal.ledger.ConcurrencyLedger;
import io.flowgate.internal.queue.GlobalAdmissionGate;
import io.flowgate.internal.queue.QueueRegistry;
import io.flowgate.policy.PriorityAwareAgingPolicy;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class DispatcherTest {

    private static QueueConfig queue(
            String id,
            int capacity,
            int reserved,
            int maximum
    ) {
        return new QueueConfig(
                id,
                Priority.NORMAL,
                1,
                capacity,
                reserved,
                maximum,
                null
        );
    }

    private static PriorityAwareAgingPolicy noAgingPolicy() {
        return new PriorityAwareAgingPolicy(
                Duration.ofSeconds(5).toNanos(),
                0,
                Map.of()
        );
    }

    private static final class Fixture {

        final AtomicLong clock = new AtomicLong(1_000L);

        final GlobalAdmissionGate gate;
        final QueueRegistry registry;
        final AdmissionController admission;
        final ConcurrencyLedger ledger;
        final DeficitRoundRobinScheduler scheduler;

        final List<Runnable> workers = new ArrayList<>();

        final Dispatcher dispatcher;

        Fixture(
                int globalConcurrency,
                int quantum,
                QueueConfig... configs
        ) {
            gate = new GlobalAdmissionGate(100);

            registry = new QueueRegistry(
                    List.of(configs),
                    gate
            );

            admission = new AdmissionController(
                    registry,
                    gate,
                    clock::get
            );

            ledger = new ConcurrencyLedger(
                    globalConcurrency,
                    registry
            );

            scheduler = new DeficitRoundRobinScheduler(
                    configs.length
            );

            Executor executor = workers::add;

            int[] quanta = new int[configs.length];

            for (int i = 0; i < quanta.length; i++) {
                quanta[i] = quantum;
            }

            dispatcher = new Dispatcher(
                    registry,
                    ledger,
                    scheduler,
                    executor,
                    clock::get,
                    quanta,
                    noAgingPolicy()
            );
        }
    }

    @Test
    void shouldDispatchAndExecuteTask() {

        Fixture f = new Fixture(
                2,
                3,
                queue("default", 10, 0, 2)
        );

        Submission<String> submission =
                f.admission.submit("default", context -> "success");

        TaskHandle<String> handle = submission.handle();

        assertTrue(f.dispatcher.dispatchOne());

        assertEquals(TaskStatus.SCHEDULED, handle.status());
        assertEquals(1, f.workers.size());
        assertEquals(1, f.ledger.activeTotal());

        // Successful handoff commits one DRR opportunity.
        assertEquals(2, f.scheduler.deficit(0));

        f.workers.remove(0).run();

        assertEquals(TaskStatus.COMPLETED, handle.status());
        assertEquals("success", handle.future().join());
        assertEquals(0, f.ledger.activeTotal());
    }

    @Test
    void shouldRespectGlobalConcurrencyLimit() {

        Fixture f = new Fixture(
                1,
                1,
                queue("first", 10, 0, 1),
                queue("second", 10, 0, 1)
        );

        f.admission.submit("first", context -> "first");
        f.admission.submit("second", context -> "second");

        assertTrue(f.dispatcher.dispatchOne());

        assertEquals(1, f.ledger.activeTotal());

        // Global capacity is exhausted.
        assertFalse(f.dispatcher.dispatchOne());

        assertEquals(1, f.workers.size());
        assertEquals(1, f.ledger.activeTotal());

        f.workers.remove(0).run();

        assertEquals(0, f.ledger.activeTotal());

        // Capacity becomes available again.
        assertTrue(f.dispatcher.dispatchOne());

        assertEquals(1, f.ledger.activeTotal());

        f.workers.remove(0).run();

        assertEquals(0, f.ledger.activeTotal());
    }

    @Test
    void shouldRespectPerQueueConcurrencyLimit() {

        Fixture f = new Fixture(
                2,
                1,
                queue("default", 10, 0, 1)
        );

        f.admission.submit("default", context -> "first");
        f.admission.submit("default", context -> "second");

        assertTrue(f.dispatcher.dispatchOne());

        assertEquals(1, f.ledger.active(0));

        // Queue maximum is one despite global capacity being two.
        assertFalse(f.dispatcher.dispatchOne());

        assertEquals(1, f.workers.size());

        f.workers.remove(0).run();

        assertEquals(0, f.ledger.active(0));

        assertTrue(f.dispatcher.dispatchOne());

        f.workers.remove(0).run();

        assertEquals(0, f.ledger.activeTotal());
    }

    @Test
    void shouldReturnFalseWhenAllQueuesAreEmpty() {

        Fixture f = new Fixture(
                2,
                1,
                queue("default", 10, 0, 2)
        );

        assertFalse(f.dispatcher.dispatchOne());

        assertEquals(0, f.ledger.activeTotal());
        assertTrue(f.workers.isEmpty());
    }

    @Test
    void shouldReleasePermitAndNotCommitDeficitOnRejection() {

        Fixture f = new Fixture(
                2,
                3,
                queue("default", 10, 0, 2)
        );

        Submission<String> submission =
                f.admission.submit("default", context -> "result");

        TaskHandle<String> handle = submission.handle();

        Executor rejectingExecutor = command -> {
            throw new RejectedExecutionException("executor full");
        };

        Dispatcher rejectingDispatcher = new Dispatcher(
                f.registry,
                f.ledger,
                f.scheduler,
                rejectingExecutor,
                f.clock::get,
                new int[]{3},
                noAgingPolicy()
        );

        assertFalse(rejectingDispatcher.dispatchOne());

        assertEquals(TaskStatus.FAILED, handle.status());
        assertEquals(0, f.ledger.activeTotal());

        // Rejected handoff must not consume DRR deficit.
        assertEquals(0, f.scheduler.deficit(0));

        assertEquals(0, f.registry.require("default").size());
    }

    @Test
    void shouldSkipCancelledTaskAndReleasePermit() {

        Fixture f = new Fixture(
                2,
                1,
                queue("default", 10, 0, 2)
        );

        Submission<String> submission =
                f.admission.submit("default", context -> "result");

        TaskHandle<String> handle = submission.handle();

        assertTrue(handle.cancel());

        assertFalse(f.dispatcher.dispatchOne());

        assertEquals(TaskStatus.CANCELLED, handle.status());
        assertEquals(0, f.ledger.activeTotal());
        assertEquals(0, f.registry.require("default").size());
        assertEquals(0, f.scheduler.deficit(0));
    }

    @Test
    void shouldExpireTaskBeforeScheduling() {

        Fixture f = new Fixture(
                2,
                1,
                queue("default", 10, 0, 2)
        );

        Submission<String> submission =
                f.admission.submit(
                        "default",
                        context -> "result",
                        Duration.ofNanos(100)
                );

        TaskHandle<String> handle = submission.handle();

        f.clock.addAndGet(100);

        assertFalse(f.dispatcher.dispatchOne());

        assertEquals(TaskStatus.EXPIRED, handle.status());
        assertEquals(0, f.ledger.activeTotal());
        assertEquals(0, f.registry.require("default").size());
        assertEquals(0, f.scheduler.deficit(0));
    }

    @Test
    void shouldReleasePermitWhenExecutorThrowsUnexpectedException() {

        Fixture f = new Fixture(
                2,
                3,
                queue("default", 10, 0, 2)
        );

        Submission<String> submission =
                f.admission.submit("default", context -> "result");

        TaskHandle<String> handle = submission.handle();

        Executor failingExecutor = command -> {
            throw new IllegalStateException("Unexpected executor failure");
        };

        Dispatcher failingDispatcher = new Dispatcher(
                f.registry,
                f.ledger,
                f.scheduler,
                failingExecutor,
                f.clock::get,
                new int[]{3},
                noAgingPolicy()
        );

        assertThrows(
                IllegalStateException.class,
                failingDispatcher::dispatchOne
        );

        assertEquals(TaskStatus.FAILED, handle.status());
        assertEquals(0, f.ledger.activeTotal());
        assertEquals(0, f.registry.require("default").size());
        assertEquals(0, f.scheduler.deficit(0));
    }

    @Test
    void shouldReleasePermitWhenTaskExecutionFails() {

        Fixture f = new Fixture(
                2,
                3,
                queue("default", 10, 0, 2)
        );

        Submission<String> submission =
                f.admission.submit(
                        "default",
                        context -> {
                            throw new IllegalStateException("Task execution failed");
                        }
                );

        TaskHandle<String> handle = submission.handle();

        assertTrue(f.dispatcher.dispatchOne());

        assertEquals(TaskStatus.SCHEDULED, handle.status());
        assertEquals(1, f.ledger.activeTotal());

        // Execute the worker task.
        Runnable worker = f.workers.remove(0);
        worker.run();

        assertEquals(TaskStatus.FAILED, handle.status());
        assertEquals(0, f.ledger.activeTotal());

        assertThrows(
                java.util.concurrent.CompletionException.class,
                () -> handle.future().join()
        );
    }
}
