package io.flowgate.internal.admission;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.flowgate.api.AdmissionOutcome;
import io.flowgate.api.Priority;
import io.flowgate.api.RejectionReason;
import io.flowgate.api.Submission;
import io.flowgate.api.TaskStatus;
import io.flowgate.config.QueueConfig;
import io.flowgate.exception.UnknownQueueException;
import io.flowgate.internal.queue.GlobalAdmissionGate;
import io.flowgate.internal.queue.QueueRegistry;
import io.flowgate.internal.queue.WorkloadQueue;
import io.flowgate.internal.task.TaskRecord;
import io.flowgate.testsupport.Concurrently;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class AdmissionControllerTest {

    static final class Fixture {
        final long[] now = {1_000L};
        final GlobalAdmissionGate gate;
        final QueueRegistry registry;
        final AdmissionController controller;

        Fixture(int globalLimit, QueueConfig... configs) {
            gate = new GlobalAdmissionGate(globalLimit);
            registry = new QueueRegistry(List.of(configs), gate);
            controller = new AdmissionController(registry, gate, () -> now[0]);
        }

        WorkloadQueue queue(String id) {
            return registry.require(id);
        }
    }

    static QueueConfig cfg(String id, int capacity) {
        return new QueueConfig(
                id,
                Priority.NORMAL,
                1,
                capacity,
                0,
                1,
                null
        );
    }
    @Test
    void acceptsWhenCapacityExists() {
        Fixture f = new Fixture(10, cfg("q", 2));
        Submission<String> s = f.controller.submit("q", ctx -> "x");
        assertTrue(s.isAccepted());
        assertEquals(AdmissionOutcome.ACCEPTED, s.outcome());
        assertEquals("q", s.handle().queueId());
        assertEquals(TaskStatus.QUEUED, s.handle().status());
        assertEquals(1, f.queue("q").size());
    }

    @Test
    void rejectsWhenQueueFull() {
        Fixture f = new Fixture(10, cfg("q", 1));
        assertTrue(f.controller.submit("q", ctx -> "x").isAccepted());
        Submission<String> s = f.controller.submit("q", ctx -> "x");
        assertEquals(AdmissionOutcome.REJECTED, s.outcome());
        assertEquals(RejectionReason.QUEUE_FULL, s.rejectionReason());
    }

    @Test
    void rejectsWhenGlobalLimitReachedAcrossQueues() {
        Fixture f = new Fixture(3, cfg("a", 5), cfg("b", 5));
        for (int i = 0; i < 2; i++) assertTrue(f.controller.submit("a", ctx -> "x").isAccepted());
        assertTrue(f.controller.submit("b", ctx -> "x").isAccepted());
        Submission<String> s = f.controller.submit("b", ctx -> "x");
        assertEquals(RejectionReason.GLOBAL_LIMIT, s.rejectionReason());
        assertEquals(1, f.queue("b").size());
    }

    @Test
    void rejectsNonPositiveDeadlines() {
        Fixture f = new Fixture(10, cfg("q", 2));
        assertEquals(RejectionReason.INVALID_TASK,
                f.controller.submit("q", ctx -> "x", Duration.ZERO).rejectionReason());
        assertEquals(RejectionReason.INVALID_TASK,
                f.controller.submit("q", ctx -> "x", Duration.ofSeconds(-1)).rejectionReason());
        assertEquals(0, f.queue("q").size());
        assertEquals(0, f.gate.held());
    }

    @Test
    void programmerErrorsThrowInsteadOfRejecting() {
        Fixture f = new Fixture(10, cfg("q", 1));
        assertThrows(UnknownQueueException.class, () -> f.controller.submit("nope", ctx -> "x"));
        assertThrows(NullPointerException.class, () -> f.controller.submit("q", null));
        assertThrows(NullPointerException.class, () -> f.controller.submit(null, ctx -> "x"));
        assertEquals(0, f.gate.held());
    }

    @Test
    void recordsDeadlineAgainstInjectedClock() {
        Fixture f = new Fixture(10, cfg("q", 1));
        f.controller.submit("q", ctx -> "x", Duration.ofSeconds(5));
        TaskRecord<?> r = f.queue("q").peek();
        assertEquals(1_000L, r.enqueueNanos());
        assertFalse(r.isExpired(1_000L + 4_999_999_999L));
        assertTrue(r.isExpired(1_000L + 5_000_000_000L));
    }

    @Test
    void hugeDeadlinesDoNotOverflow() {
        Fixture f = new Fixture(10, cfg("q", 1));
        assertTrue(f.controller.submit("q", ctx -> "x", Duration.ofSeconds(Long.MAX_VALUE / 2)).isAccepted());
        assertFalse(f.queue("q").peek().isExpired(1_000L));
    }

    // ---- result object -------------------------------------------------------------------

    @Test
    void rejectionsAreSharedInstancesAndGuardAccessors() {
        Fixture f = new Fixture(10, cfg("q", 1));
        Submission<String> accepted = f.controller.submit("q", ctx -> "x");
        Submission<String> r1 = f.controller.submit("q", ctx -> "x");
        Submission<String> r2 = f.controller.submit("q", ctx -> "x");
        assertSame(r1, r2);
        assertThrows(IllegalStateException.class, r1::handle);
        assertThrows(IllegalStateException.class, accepted::rejectionReason);
    }

    // ---- shutdown ------------------------------------------------------------------------

    @Test
    void rejectsAfterShutdownAndShutdownWinsOverQueueFull() {
        Fixture f = new Fixture(10, cfg("q", 1));
        f.controller.submit("q", ctx -> "x"); // queue now full
        f.controller.shutdown();
        f.controller.shutdown(); // idempotent
        assertFalse(f.controller.isAccepting());
        assertEquals(RejectionReason.SHUTDOWN, f.controller.submit("q", ctx -> "x").rejectionReason());
        assertEquals(1, f.queue("q").size()); // already-accepted work is untouched
    }

    @Test
    void shutdownIsLinearizableWithConcurrentSubmitters() throws Exception {
        Fixture f = new Fixture(500_000, cfg("q", 500_000));
        WorkloadQueue q = f.queue("q");
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger violations = new AtomicInteger();
        AtomicBoolean shutdownReturned = new AtomicBoolean();

        Thread closer = new Thread(() -> {
            long deadline = System.nanoTime() + 20_000_000_000L;
            while (accepted.get() < 1_000 && System.nanoTime() - deadline < 0) Thread.onSpinWait();
            f.controller.shutdown();
            shutdownReturned.set(true);
        });
        closer.setDaemon(true);
        closer.start();

        Concurrently.run(6, t -> {
            int postShutdown = 0;
            while (postShutdown < 200) {
                boolean wasClosed = shutdownReturned.get(); // read BEFORE submitting
                Submission<String> s = f.controller.submit("q", ctx -> "x");
                if (s.isAccepted()) {
                    accepted.incrementAndGet();
                    if (wasClosed) violations.incrementAndGet(); // admitted after shutdown returned
                } else if (wasClosed && s.rejectionReason() != RejectionReason.SHUTDOWN) {
                    violations.incrementAndGet();
                }
                if (wasClosed) postShutdown++;
            }
        });
        closer.join(30_000);

        assertEquals(0, violations.get());
        int drained = 0;
        long giveUp = System.nanoTime() + 20_000_000_000L;
        while (f.gate.held() > 0 && System.nanoTime() - giveUp < 0) { // submitters finished: held == queued
            if (q.poll() != null) drained++;
            else Thread.onSpinWait();
        }
        assertEquals(0, f.gate.held(), "permits leaked or tasks lost");
        assertEquals(accepted.get(), drained);
    }

    // ---- concurrency ---------------------------------------------------------------------

    @Test
    void concurrentSubmittersAcceptExactlyCapacity() throws Exception {
        Fixture f = new Fixture(10_000, cfg("q", 1_000));
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger queueFull = new AtomicInteger();
        AtomicInteger other = new AtomicInteger();
        Concurrently.run(16, t -> {
            for (int i = 0; i < 500; i++) {
                Submission<String> s = f.controller.submit("q", ctx -> "x");
                if (s.isAccepted()) accepted.incrementAndGet();
                else if (s.rejectionReason() == RejectionReason.QUEUE_FULL) queueFull.incrementAndGet();
                else other.incrementAndGet();
            }
        });
        assertEquals(1_000, accepted.get());
        assertEquals(16 * 500 - 1_000, queueFull.get());
        assertEquals(0, other.get());
        assertEquals(1_000, f.queue("q").size());
        assertEquals(1_000, f.gate.held());
    }

    @Test
    void producersAndConsumerKeepQueueWithinBoundsAndLeakNothing() throws Exception {
        Fixture f = new Fixture(10_000, cfg("q", 64));
        WorkloadQueue q = f.queue("q");
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger polled = new AtomicInteger();
        AtomicInteger maxSeen = new AtomicInteger();
        AtomicBoolean producersDone = new AtomicBoolean();

        Thread consumer = new Thread(() -> {
            long giveUp = System.nanoTime() + 30_000_000_000L;
            while (System.nanoTime() - giveUp < 0) {
                maxSeen.accumulateAndGet(q.size(), Math::max);
                if (q.poll() != null) polled.incrementAndGet();
                else if (producersDone.get() && f.gate.held() == 0) break;
                else Thread.onSpinWait();
            }
        });
        consumer.setDaemon(true); // a failing run must not keep the JVM alive
        consumer.start();
        Concurrently.run(8, t -> {
            for (int i = 0; i < 5_000; i++) {
                if (f.controller.submit("q", ctx -> "x").isAccepted()) accepted.incrementAndGet();
            }
        });
        producersDone.set(true);
        consumer.join(30_000);

        assertFalse(consumer.isAlive());
        assertEquals(accepted.get(), polled.get());
        assertTrue(maxSeen.get() <= 64, "queue exceeded capacity: " + maxSeen.get());
        assertEquals(0, q.size());
        assertEquals(0, f.gate.held());
    }

    @Test
    void releasesReservationWhenTaskCreationFails() {

        GlobalAdmissionGate gate = new GlobalAdmissionGate(10);

        QueueRegistry registry = new QueueRegistry(
                List.of(cfg("q", 2)),
                gate
        );

        AdmissionController controller = new AdmissionController(
                registry,
                gate,
                () -> {
                    throw new IllegalStateException("Clock failure");
                }
        );

        WorkloadQueue queue = registry.require("q");

        assertThrows(
                IllegalStateException.class,
                () -> controller.submit("q", ctx -> "result")
        );

        // Local queue reservation must be released.
        assertEquals(0, queue.size());

        // Global admission reservation must also be released.
        assertEquals(0, gate.held());

        // The queue must still be usable after the failure.
        Submission<String> retry =
                new AdmissionController(
                        registry,
                        gate,
                        () -> 1_000L
                ).submit("q", ctx -> "success");

        assertTrue(retry.isAccepted());
        assertEquals(1, queue.size());
        assertEquals(1, gate.held());
    }

}
