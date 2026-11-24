
package io.flowgate.internal.queue;

import io.flowgate.api.Priority;
import io.flowgate.config.QueueConfig;
import io.flowgate.internal.queue.WorkloadQueue.Reservation;
import io.flowgate.internal.task.TaskRecord;
import io.flowgate.testsupport.Concurrently;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class WorkloadQueueTest {

    private static WorkloadQueue queue(int capacity, GlobalAdmissionGate gate) {
        QueueConfig config = new QueueConfig(
                "q",
                Priority.NORMAL,
                1,
                capacity,
                0,
                1,
                null
        );

        return new WorkloadQueue(0, config, gate);
    }

    private static boolean offer(WorkloadQueue queue, long id) {
        if (queue.tryReserve() != Reservation.RESERVED) {
            return false;
        }

        queue.enqueueReserved(
                new TaskRecord<String>(
                        id,
                        "q",
                        ctx -> "x",
                        0L,
                        false,
                        0L
                )
        );

        return true;
    }
    @Test
    void concurrentProducersAndConsumersDoNotLoseOrDuplicateTasks()
            throws Exception {

        int producers = 8;
        int consumers = 4;
        int tasksPerProducer = 500;
        int capacity = 128;

        GlobalAdmissionGate gate = new GlobalAdmissionGate(capacity);
        WorkloadQueue queue = queue(capacity, gate);

        ExecutorService executor =
                Executors.newFixedThreadPool(producers + consumers);

        CountDownLatch ready =
                new CountDownLatch(producers + consumers);
        CountDownLatch start = new CountDownLatch(1);

        AtomicInteger producersRemaining = new AtomicInteger(producers);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger consumed = new AtomicInteger();
        AtomicLong ids = new AtomicLong();

        Set<Long> consumedIds = ConcurrentHashMap.newKeySet();

        Future<?>[] futures = new Future<?>[producers + consumers];

        try {
            // Start consumers.
            for (int i = 0; i < consumers; i++) {
                futures[i] = executor.submit(() -> {
                    ready.countDown();

                    if (!start.await(10, TimeUnit.SECONDS)) {
                        throw new AssertionError("Start barrier timed out");
                    }

                    while (true) {
                        TaskRecord<?> record = queue.poll();

                        if (record != null) {
                            if (!consumedIds.add(record.id())) {
                                throw new AssertionError(
                                        "Duplicate task consumed: " + record.id()
                                );
                            }

                            consumed.incrementAndGet();
                            continue;
                        }

                        // A null poll does not necessarily mean the queue
                        // is empty: a producer may hold an unpublished slot.
                        if (producersRemaining.get() == 0
                                && queue.size() == 0) {
                            break;
                        }

                        Thread.yield();
                    }

                    return null;
                });
            }

            // Start producers.
            for (int i = 0; i < producers; i++) {
                futures[consumers + i] = executor.submit(() -> {
                    ready.countDown();

                    try {
                        if (!start.await(10, TimeUnit.SECONDS)) {
                            throw new AssertionError(
                                    "Start barrier timed out"
                            );
                        }

                        for (int j = 0; j < tasksPerProducer; j++) {
                            long id = ids.incrementAndGet();

                            while (!offer(queue, id)) {
                                Thread.yield();
                            }

                            accepted.incrementAndGet();
                        }

                        return null;
                    } finally {
                        producersRemaining.decrementAndGet();
                    }
                });
            }

            assertTrue(
                    ready.await(10, TimeUnit.SECONDS),
                    "Workers did not become ready"
            );

            start.countDown();

            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }

        } finally {
            start.countDown();
            executor.shutdownNow();

            assertTrue(
                    executor.awaitTermination(10, TimeUnit.SECONDS),
                    "Executor did not terminate"
            );
        }

        int expectedTasks = producers * tasksPerProducer;

        assertEquals(expectedTasks, accepted.get());
        assertEquals(expectedTasks, consumed.get());
        assertEquals(expectedTasks, consumedIds.size());

        assertEquals(0, queue.size());
        assertEquals(0, gate.held());
    }

    @Test
    void isBoundedByCapacity() {
        WorkloadQueue queue = queue(3, new GlobalAdmissionGate(100));

        assertTrue(offer(queue, 1));
        assertTrue(offer(queue, 2));
        assertTrue(offer(queue, 3));

        assertEquals(Reservation.QUEUE_FULL, queue.tryReserve());
        assertEquals(3, queue.size());
    }

    @Test
    void pollIsFifoAndFreesSlots() {
        GlobalAdmissionGate gate = new GlobalAdmissionGate(100);
        WorkloadQueue queue = queue(3, gate);

        offer(queue, 1);
        offer(queue, 2);
        offer(queue, 3);

        assertEquals(1L, queue.poll().id());
        assertEquals(2, queue.size());
        assertEquals(2, gate.held());

        assertTrue(offer(queue, 4));

        assertEquals(2L, queue.poll().id());
        assertEquals(3L, queue.poll().id());
        assertEquals(4L, queue.poll().id());

        assertTrue(queue.poll() == null);
        assertEquals(0, queue.size());
        assertEquals(0, gate.held());
    }

    @Test
    void abortReservationRestoresBothSlots() {
        GlobalAdmissionGate gate = new GlobalAdmissionGate(100);
        WorkloadQueue queue = queue(1, gate);

        assertEquals(Reservation.RESERVED, queue.tryReserve());
        assertEquals(Reservation.QUEUE_FULL, queue.tryReserve());

        queue.abortReservation();

        assertEquals(0, queue.size());
        assertEquals(0, gate.held());
        assertEquals(Reservation.RESERVED, queue.tryReserve());
    }

    @Test
    void globalLimitIsReportedAndQueueSlotRolledBack() {
        GlobalAdmissionGate gate = new GlobalAdmissionGate(2);
        WorkloadQueue queue = queue(5, gate);

        assertTrue(offer(queue, 1));
        assertTrue(offer(queue, 2));

        assertEquals(Reservation.GLOBAL_LIMIT, queue.tryReserve());
        assertEquals(2, queue.size());
        assertEquals(2, gate.held());
    }

    @Test
    void closedGateIsReportedAndQueueSlotRolledBack() {
        GlobalAdmissionGate gate = new GlobalAdmissionGate(10);
        WorkloadQueue queue = queue(5, gate);

        gate.close();

        assertEquals(Reservation.CLOSED, queue.tryReserve());
        assertEquals(0, queue.size());
    }

    @Test
    void concurrentProducersNeverExceedCapacity() throws Exception {
        GlobalAdmissionGate gate = new GlobalAdmissionGate(10_000);
        WorkloadQueue queue = queue(100, gate);

        AtomicInteger accepted = new AtomicInteger();
        AtomicLong ids = new AtomicLong();

        Concurrently.run(8, workerId -> {
            for (int i = 0; i < 500; i++) {
                if (offer(queue, ids.incrementAndGet())) {
                    accepted.incrementAndGet();
                }
            }
        });

        assertEquals(100, accepted.get());
        assertEquals(100, queue.size());
        assertEquals(100, gate.held());
    }
    @Test
    void failedPublicationCanBeRolledBackWithoutLeakingCapacity() {
        GlobalAdmissionGate gate = new GlobalAdmissionGate(1);
        WorkloadQueue queue = queue(1, gate);

        assertEquals(
                WorkloadQueue.Reservation.RESERVED,
                queue.tryReserve()
        );

        assertEquals(1, queue.size());
        assertEquals(1, gate.held());

        // Null publication fails before the task becomes visible.
        assertThrows(
                NullPointerException.class,
                () -> queue.enqueueReserved(null)
        );

        // The caller must explicitly abort the outstanding reservation.
        queue.abortReservation();

        assertEquals(0, queue.size());
        assertEquals(0, gate.held());

        // Verify that the released capacity is reusable.
        assertEquals(
                WorkloadQueue.Reservation.RESERVED,
                queue.tryReserve()
        );

        queue.abortReservation();

        assertEquals(0, queue.size());
        assertEquals(0, gate.held());
    }

    @Test
    void removeExactQueuedRecordReleasesCapacityExactlyOnce() {

        GlobalAdmissionGate gate = new GlobalAdmissionGate(10);
        WorkloadQueue queue = queue(3, gate);

        assertTrue(offer(queue, 1));
        assertTrue(offer(queue, 2));

        TaskRecord<?> first = queue.peek();
        assertNotNull(first);

        // First removal succeeds.
        assertTrue(queue.remove(first));

        // Second removal must not release capacity again.
        assertFalse(queue.remove(first));

        assertEquals(1, queue.size());
        assertEquals(1, gate.held());

        // Remaining task is still available.
        assertEquals(2L, queue.poll().id());

        assertEquals(0, queue.size());
        assertEquals(0, gate.held());
    }

    @Test
    void removeDoesNotRemoveDifferentRecordWithSameId() {

        GlobalAdmissionGate gate = new GlobalAdmissionGate(10);
        WorkloadQueue queue = queue(2, gate);

        assertTrue(offer(queue, 7));

        TaskRecord<?> queued = queue.peek();

        TaskRecord<?> different = new TaskRecord<>(
                7,
                "q",
                ctx -> "different",
                0L,
                false,
                0L
        );

        // Same ID, but a different object.
        assertFalse(queue.remove(different));

        assertSame(queued, queue.peek());
        assertEquals(1, queue.size());
        assertEquals(1, gate.held());
    }

    @Test
    void removeRacingWithPollReleasesReservationExactlyOnce()
            throws Exception {

        GlobalAdmissionGate gate = new GlobalAdmissionGate(1);
        WorkloadQueue queue = queue(1, gate);

        assertTrue(offer(queue, 42));

        TaskRecord<?> record = queue.peek();
        assertNotNull(record);

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            Future<Boolean> removal = executor.submit(() -> {
                assertTrue(start.await(5, TimeUnit.SECONDS));
                return queue.remove(record);
            });

            Future<TaskRecord<?>> polling = executor.submit(() -> {
                assertTrue(start.await(5, TimeUnit.SECONDS));
                return queue.poll();
            });

            start.countDown();

            boolean removed = removal.get(5, TimeUnit.SECONDS);
            TaskRecord<?> polled = polling.get(5, TimeUnit.SECONDS);

            // Exactly one operation must own the record.
            assertTrue(
                    removed ^ (polled != null),
                    "Exactly one of remove or poll must own the record"
            );

            assertEquals(0, queue.size());
            assertEquals(0, gate.held());

        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void snapshotReturnsImmutableCopyWithoutChangingCapacity() {

        GlobalAdmissionGate gate = new GlobalAdmissionGate(10);
        WorkloadQueue queue = queue(3, gate);

        assertTrue(offer(queue, 1));
        assertTrue(offer(queue, 2));

        List<TaskRecord<?>> snapshot = queue.snapshot();

        assertEquals(2, snapshot.size());
        assertEquals(2, queue.size());
        assertEquals(2, gate.held());

        assertThrows(
                UnsupportedOperationException.class,
                snapshot::clear
        );

        // Snapshot creation must not affect the actual queue.
        assertEquals(2, queue.size());
        assertEquals(2, gate.held());
    }


}
