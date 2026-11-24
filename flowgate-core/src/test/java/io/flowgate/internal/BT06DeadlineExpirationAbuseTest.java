package io.flowgate.internal.BT06;

import io.flowgate.api.Priority;
import io.flowgate.api.TaskStatus;
import io.flowgate.config.QueueConfig;
import io.flowgate.internal.deadline.DeadlineManager;
import io.flowgate.internal.queue.GlobalAdmissionGate;
import io.flowgate.internal.queue.QueueRegistry;
import io.flowgate.internal.queue.WorkloadQueue;
import io.flowgate.internal.task.TaskRecord;
import io.flowgate.internal.time.TimeSource;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class BT06DeadlineExpirationAbuseTest {

    private static final class Fixture {

        final AtomicLong clock = new AtomicLong(100L);

        final TimeSource time = clock::get;

        final GlobalAdmissionGate gate =
                new GlobalAdmissionGate(1000);

        final QueueConfig config = new QueueConfig(
                "normal",
                Priority.NORMAL,
                1,
                1000,
                0,
                10,
                null
        );

        final QueueRegistry registry =
                new QueueRegistry(List.of(config), gate);

        final WorkloadQueue queue =
                registry.require("normal");

        final DeadlineManager manager =
                new DeadlineManager(registry, time);

        TaskRecord<String> enqueue(
                long id,
                boolean hasDeadline,
                long deadlineNanos
        ) {

            TaskRecord<String> record = new TaskRecord<>(
                    id,
                    "normal",
                    context -> "completed",
                    clock.get(),
                    hasDeadline,
                    deadlineNanos
            );

            assertEquals(
                    WorkloadQueue.Reservation.RESERVED,
                    queue.tryReserve()
            );

            assertTrue(record.markQueued());

            queue.enqueueReserved(record);

            return record;
        }
    }

    // --------------------------------------------------
    // BT06-01: Deadline boundary precision
    // --------------------------------------------------

    @Test
    void deadlineBoundaryPrecision() {

        Fixture fixture = new Fixture();

        TaskRecord<String> record =
                fixture.enqueue(1L, true, 200L);

        // One tick before deadline
        fixture.clock.set(199L);

        assertEquals(0, fixture.manager.sweep());
        assertEquals(TaskStatus.QUEUED, record.status());
        assertEquals(1, fixture.gate.held());

        // Exactly at deadline
        fixture.clock.set(200L);

        assertEquals(1, fixture.manager.sweep());

        assertEquals(TaskStatus.EXPIRED, record.status());
        assertEquals(0, fixture.queue.size());
        assertEquals(0, fixture.gate.held());
    }

    // --------------------------------------------------
    // BT06-02: Repeated sweeps
    // --------------------------------------------------

    @Test
    void repeatedSweepsMustNotExpireOrReleaseTwice() {

        Fixture fixture = new Fixture();

        TaskRecord<String> record =
                fixture.enqueue(2L, true, 200L);

        fixture.clock.set(500L);

        assertEquals(1, fixture.manager.sweep());

        for (int i = 0; i < 1000; i++) {
            assertEquals(0, fixture.manager.sweep());
        }

        assertEquals(TaskStatus.EXPIRED, record.status());
        assertEquals(0, fixture.queue.size());
        assertEquals(0, fixture.gate.held());
    }

    // --------------------------------------------------
    // BT06-03: Concurrent sweeper pressure
    // --------------------------------------------------

    @Test
    void concurrentSweepsMustHaveSingleExpirationWinner()
            throws Exception {

        for (int iteration = 0; iteration < 100; iteration++) {

            Fixture fixture = new Fixture();

            TaskRecord<String> record =
                    fixture.enqueue(iteration + 10L, true, 200L);

            fixture.clock.set(300L);

            int threads = 8;

            ExecutorService executor =
                    Executors.newFixedThreadPool(threads);

            CountDownLatch ready =
                    new CountDownLatch(threads);

            CountDownLatch start =
                    new CountDownLatch(1);

            List<Future<Integer>> results =
                    new java.util.ArrayList<>();

            try {

                for (int i = 0; i < threads; i++) {

                    results.add(executor.submit(() -> {

                        ready.countDown();
                        start.await();

                        return fixture.manager.sweep();
                    }));
                }

                assertTrue(ready.await(3, TimeUnit.SECONDS));

                start.countDown();

                int totalExpired = 0;

                for (Future<Integer> result : results) {
                    totalExpired += result.get(
                            3,
                            TimeUnit.SECONDS
                    );
                }

                // Exactly one concurrent sweep can win.
                assertEquals(1, totalExpired);

                assertEquals(TaskStatus.EXPIRED, record.status());
                assertEquals(0, fixture.queue.size());
                assertEquals(0, fixture.gate.held());

            } finally {
                executor.shutdownNow();
            }
        }
    }

    // --------------------------------------------------
    // BT06-04: Expiration vs cancellation
    // --------------------------------------------------

    @Test
    void expirationMustNotOverrideCancellation() {

        Fixture fixture = new Fixture();

        TaskRecord<String> record =
                fixture.enqueue(500L, true, 200L);

        assertTrue(record.cancel());

        fixture.clock.set(300L);

        assertEquals(0, fixture.manager.sweep());

        assertEquals(TaskStatus.CANCELLED, record.status());

        // Existing behavior: DeadlineManager ignores cancelled
        // records; queue cleanup is handled by queue lifecycle.
        assertEquals(1, fixture.queue.size());
        assertEquals(1, fixture.gate.held());
    }

    // --------------------------------------------------
    // BT06-05: Expiration vs dispatcher poll
    // --------------------------------------------------

    @Test
    void expirationVsPollMustReleaseCapacityExactlyOnce()
            throws Exception {

        for (int iteration = 0; iteration < 500; iteration++) {

            Fixture fixture = new Fixture();

            TaskRecord<String> record =
                    fixture.enqueue(iteration + 1000L, true, 200L);

            fixture.clock.set(300L);

            ExecutorService executor =
                    Executors.newFixedThreadPool(2);

            CountDownLatch start =
                    new CountDownLatch(1);

            try {

                Future<Integer> sweep =
                        executor.submit(() -> {
                            start.await();
                            return fixture.manager.sweep();
                        });

                Future<TaskRecord<?>> poll =
                        executor.submit(() -> {
                            start.await();
                            return fixture.queue.poll();
                        });

                start.countDown();

                int expired =
                        sweep.get(3, TimeUnit.SECONDS);

                TaskRecord<?> polled =
                        poll.get(3, TimeUnit.SECONDS);

                assertTrue(expired == 0 || expired == 1);

                assertEquals(0, fixture.queue.size());
                assertEquals(0, fixture.gate.held());

                assertTrue(
                        record.status() == TaskStatus.EXPIRED
                                || record.status() == TaskStatus.QUEUED
                );

                if (polled != null) {
                    assertSame(record, polled);
                }

            } finally {
                executor.shutdownNow();
            }
        }
    }

    // --------------------------------------------------
    // BT06-06: Mixed deadline flood
    // --------------------------------------------------

    @Test
    void mixedDeadlineFloodExpiresOnlyOverdueTasks() {

        Fixture fixture = new Fixture();

        int total = 100;

        int expectedExpired = 0;

        for (int i = 0; i < total; i++) {

            boolean hasDeadline = i % 5 != 0;

            long deadline = (i % 2 == 0)
                    ? 150L
                    : 400L;

            fixture.enqueue(
                    i + 2000L,
                    hasDeadline,
                    deadline
            );

            if (hasDeadline && deadline <= 300L) {
                expectedExpired++;
            }
        }

        fixture.clock.set(300L);

        assertEquals(
                expectedExpired,
                fixture.manager.sweep()
        );

        assertEquals(
                total - expectedExpired,
                fixture.queue.size()
        );

        assertEquals(
                total - expectedExpired,
                fixture.gate.held()
        );
    }

    // --------------------------------------------------
    // BT06-07: Capacity recycling after repeated expirations
    // --------------------------------------------------

    @Test
    void capacityMustRecycleAcrossRepeatedExpirationCycles() {

        Fixture fixture = new Fixture();

        int cycles = 100;

        for (int cycle = 0; cycle < cycles; cycle++) {

            long id = 10_000L + cycle;

            long deadline = fixture.clock.get() + 10L;

            TaskRecord<String> record =
                    fixture.enqueue(id, true, deadline);

            fixture.clock.set(deadline + 1L);

            assertEquals(1, fixture.manager.sweep());

            assertEquals(TaskStatus.EXPIRED, record.status());
            assertEquals(0, fixture.queue.size());
            assertEquals(0, fixture.gate.held());

            // Move clock forward before the next cycle.
            fixture.clock.addAndGet(10L);
        }
    }

    // --------------------------------------------------
    // BT06-08: Deterministic randomized deadline abuse
    // --------------------------------------------------

    @Test
    void randomizedDeadlineAbuseMaintainsQueueAndCapacityIntegrity() {

        Fixture fixture = new Fixture();

        Random random = new Random(6062026L);

        int total = 500;

        int expectedExpired = 0;

        long now = 10_000L;

        for (int i = 0; i < total; i++) {

            boolean hasDeadline = random.nextInt(10) != 0;

            // Deadlines randomly range from overdue to future.
            long deadline = now + random.nextInt(2000) - 1000;

            fixture.enqueue(
                    50_000L + i,
                    hasDeadline,
                    deadline
            );

            if (hasDeadline && deadline <= now) {
                expectedExpired++;
            }
        }

        fixture.clock.set(now);

        assertEquals(
                expectedExpired,
                fixture.manager.sweep()
        );

        assertEquals(
                total - expectedExpired,
                fixture.queue.size()
        );

        assertEquals(
                total - expectedExpired,
                fixture.gate.held()
        );

        // Repeated sweeps must remain idempotent.
        for (int i = 0; i < 50; i++) {
            assertEquals(0, fixture.manager.sweep());
        }

        assertEquals(
                total - expectedExpired,
                fixture.queue.size()
        );

        assertEquals(
                total - expectedExpired,
                fixture.gate.held()
        );
    }
}