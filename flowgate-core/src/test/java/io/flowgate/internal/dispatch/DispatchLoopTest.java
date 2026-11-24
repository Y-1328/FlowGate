
package io.flowgate.internal.dispatch;

import io.flowgate.api.Priority;
import io.flowgate.api.Submission;
import io.flowgate.api.TaskStatus;
import io.flowgate.config.QueueConfig;
import io.flowgate.internal.admission.AdmissionController;
import io.flowgate.internal.ledger.ConcurrencyLedger;
import io.flowgate.internal.queue.GlobalAdmissionGate;
import io.flowgate.internal.queue.QueueRegistry;
import io.flowgate.internal.time.TimeSource;
import io.flowgate.policy.PriorityAwareAgingPolicy;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class DispatchLoopTest {

    private PriorityAwareAgingPolicy noAgingPolicy() {
        return new PriorityAwareAgingPolicy(
                Duration.ofSeconds(5).toNanos(),
                0,
                Map.of()
        );
    }

    private Dispatcher createDispatcher() {

        QueueConfig config = new QueueConfig(
                "default",
                Priority.NORMAL,
                1,
                10,
                0,
                2,
                null
        );

        GlobalAdmissionGate gate = new GlobalAdmissionGate(10);

        QueueRegistry registry = new QueueRegistry(
                List.of(config),
                gate
        );

        ConcurrencyLedger ledger = new ConcurrencyLedger(
                2,
                registry
        );

        DeficitRoundRobinScheduler scheduler =
                new DeficitRoundRobinScheduler(1);

        Executor executor = Runnable::run;

        TimeSource time = System::nanoTime;

        return new Dispatcher(
                registry,
                ledger,
                scheduler,
                executor,
                time,
                new int[]{1},
                noAgingPolicy()
        );
    }

    @Test
    void shouldStartAndStopDispatcher() {

        DispatchLoop loop = new DispatchLoop(
                createDispatcher(),
                Duration.ofMillis(2)
        );

        loop.start();

        assertTrue(loop.isRunning());

        loop.close();

        assertFalse(loop.isRunning());
    }

    @Test
    void shouldNotAllowStartingTwice() {

        DispatchLoop loop = new DispatchLoop(
                createDispatcher(),
                Duration.ofMillis(2)
        );

        loop.start();

        try {
            assertThrows(
                    IllegalStateException.class,
                    loop::start
            );
        } finally {
            loop.close();
        }
    }

    @Test
    void shouldAllowSignalBeforeStart() {

        DispatchLoop loop = new DispatchLoop(
                createDispatcher(),
                Duration.ofMillis(2)
        );

        assertDoesNotThrow(loop::signal);
    }

    @Test
    void shouldRejectInvalidIdleWait() {

        assertThrows(
                IllegalArgumentException.class,
                () -> new DispatchLoop(
                        createDispatcher(),
                        Duration.ZERO
                )
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> new DispatchLoop(
                        createDispatcher(),
                        Duration.ofNanos(-1)
                )
        );
    }

    @Test
    void shouldNotAllowStartingAfterClose() {

        DispatchLoop loop = new DispatchLoop(
                createDispatcher(),
                Duration.ofMillis(2)
        );

        loop.close();

        assertFalse(loop.isRunning());

        assertThrows(
                IllegalStateException.class,
                loop::start
        );
    }

    @Test
    void closeShouldBeIdempotent() {

        DispatchLoop loop = new DispatchLoop(
                createDispatcher(),
                Duration.ofMillis(2)
        );

        loop.start();

        assertDoesNotThrow(loop::close);
        assertDoesNotThrow(loop::close);

        assertFalse(loop.isRunning());
    }

    @Test
    void shouldDispatchSubmittedTaskAfterSignal() throws Exception {

        QueueConfig config = new QueueConfig(
                "default",
                Priority.NORMAL,
                1,
                10,
                0,
                2,
                null
        );

        GlobalAdmissionGate gate = new GlobalAdmissionGate(10);

        QueueRegistry registry = new QueueRegistry(
                List.of(config),
                gate
        );

        TimeSource time = System::nanoTime;

        AdmissionController admission = new AdmissionController(
                registry,
                gate,
                time
        );

        ConcurrencyLedger ledger = new ConcurrencyLedger(
                2,
                registry
        );

        DeficitRoundRobinScheduler scheduler =
                new DeficitRoundRobinScheduler(1);

        Executor executor = Runnable::run;

        Dispatcher dispatcher = new Dispatcher(
                registry,
                ledger,
                scheduler,
                executor,
                time,
                new int[]{1},
                noAgingPolicy()
        );

        DispatchLoop loop = new DispatchLoop(
                dispatcher,
                Duration.ofSeconds(5)
        );

        CountDownLatch executed = new CountDownLatch(1);

        try {
            loop.start();

            Submission<String> submission = admission.submit(
                    "default",
                    context -> {
                        executed.countDown();
                        return "success";
                    }
            );

            assertTrue(submission.isAccepted());

            // Wake the idle dispatcher.
            loop.signal();

            assertTrue(
                    executed.await(3, TimeUnit.SECONDS),
                    "Dispatcher did not execute the submitted task"
            );

            assertEquals(
                    "success",
                    submission.handle().future().get(3, TimeUnit.SECONDS)
            );

            // The future is completed before the worker's finally releases the
            // permit, so wait (bounded) for the release instead of racing it.
            long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);

            while (ledger.activeTotal() != 0
                    && System.nanoTime() - limit < 0) {
                Thread.onSpinWait();
            }

            assertEquals(0, ledger.activeTotal());

        } finally {
            loop.close();
        }
    }

    @Test
    void shouldRecordUnexpectedDispatcherFailure() throws Exception {

        QueueConfig config = new QueueConfig(
                "default",
                Priority.NORMAL,
                1,
                10,
                0,
                2,
                null
        );

        GlobalAdmissionGate gate = new GlobalAdmissionGate(10);

        QueueRegistry registry = new QueueRegistry(
                List.of(config),
                gate
        );

        TimeSource time = System::nanoTime;

        AdmissionController admission = new AdmissionController(
                registry,
                gate,
                time
        );

        ConcurrencyLedger ledger = new ConcurrencyLedger(
                2,
                registry
        );

        DeficitRoundRobinScheduler scheduler =
                new DeficitRoundRobinScheduler(1);

        IllegalStateException expectedFailure =
                new IllegalStateException("Simulated dispatcher failure");

        Executor failingExecutor = command -> {
            throw expectedFailure;
        };

        Dispatcher dispatcher = new Dispatcher(
                registry,
                ledger,
                scheduler,
                failingExecutor,
                time,
                new int[]{1},
                noAgingPolicy()
        );

        DispatchLoop loop = new DispatchLoop(
                dispatcher,
                Duration.ofMillis(5)
        );

        try {
            Submission<String> submission = admission.submit(
                    "default",
                    context -> "result"
            );

            assertTrue(submission.isAccepted());

            loop.start();

            // Wait for the dispatcher thread to record its failure.
            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(3);

            while (loop.failure() == null
                    && System.nanoTime() - deadline < 0) {
                Thread.onSpinWait();
            }

            assertSame(
                    expectedFailure,
                    loop.failure(),
                    "Unexpected dispatcher failure was not recorded"
            );

            assertFalse(
                    loop.isRunning(),
                    "Dispatcher should terminate after an unexpected failure"
            );

            assertEquals(
                    TaskStatus.FAILED,
                    submission.handle().status()
            );

            assertEquals(0, ledger.activeTotal());

        } finally {
            loop.close();
        }
    }
}
