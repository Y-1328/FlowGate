
package io.flowgate.internal.deadline;

import io.flowgate.api.Priority;
import io.flowgate.api.TaskStatus;
import io.flowgate.config.QueueConfig;
import io.flowgate.internal.queue.GlobalAdmissionGate;
import io.flowgate.internal.queue.QueueRegistry;
import io.flowgate.internal.queue.WorkloadQueue;
import io.flowgate.internal.task.TaskRecord;
import io.flowgate.internal.time.TimeSource;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class DeadlineManagerTest {

    private static final class Fixture {

        final AtomicLong clock = new AtomicLong(100L);

        final TimeSource time = clock::get;

        final GlobalAdmissionGate gate =
                new GlobalAdmissionGate(10);

        final QueueConfig config = new QueueConfig(
                "normal",
                Priority.NORMAL,
                1,
                10,
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

    @Test
    void expiresTaskAfterDeadlineAndReleasesCapacity() {

        Fixture fixture = new Fixture();

        TaskRecord<String> record =
                fixture.enqueue(1L, true, 200L);

        // Current time is 100; deadline is 200.
        fixture.clock.set(201L);

        assertEquals(1, fixture.manager.sweep());

        assertEquals(TaskStatus.EXPIRED, record.status());
        assertEquals(0, fixture.queue.size());
        assertEquals(0, fixture.gate.held());

        CompletionException exception = assertThrows(
                CompletionException.class,
                () -> record.future().join()
        );

        assertInstanceOf(
                TimeoutException.class,
                exception.getCause()
        );

        // A second sweep must not expire or release it again.
        assertEquals(0, fixture.manager.sweep());
        assertEquals(0, fixture.gate.held());
    }




    @Test
    void shouldReleaseCapacityExactlyOnceWhenSweepRacesWithDispatcherPoll()
            throws Exception {

        for (int iteration = 0; iteration < 500; iteration++) {

            Fixture fixture = new Fixture();

            TaskRecord<String> record =
                    fixture.enqueue(iteration + 100L, true, 200L);

            fixture.clock.set(201L);

            CountDownLatch start = new CountDownLatch(1);

            ExecutorService executor = Executors.newFixedThreadPool(2);

            try {

                Future<Integer> sweepResult = executor.submit(() -> {
                    start.await();
                    return fixture.manager.sweep();
                });

                Future<TaskRecord<?>> pollResult = executor.submit(() -> {
                    start.await();
                    return fixture.queue.poll();
                });

                // Release both competing operations together.
                start.countDown();

                int expired = sweepResult.get(3, TimeUnit.SECONDS);
                TaskRecord<?> polled = pollResult.get(3, TimeUnit.SECONDS);

                assertTrue(expired == 0 || expired == 1);

                // Either the sweeper or the dispatcher-side poll
                // must have removed the published queue record.
                assertEquals(0, fixture.queue.size());

                // Exactly one admission reservation must be released.
                assertEquals(0, fixture.gate.held());

                // The record can only be expired or still queued if
                // the dispatcher won the poll before the sweeper saw it.
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

    @Test
    void doesNotExpireTaskBeforeDeadline() {

        Fixture fixture = new Fixture();

        TaskRecord<String> record =
                fixture.enqueue(2L, true, 200L);

        fixture.clock.set(199L);

        assertEquals(0, fixture.manager.sweep());

        assertEquals(TaskStatus.QUEUED, record.status());
        assertEquals(1, fixture.queue.size());
        assertEquals(1, fixture.gate.held());
    }

    @Test
    void doesNotExpireTaskWithoutDeadline() {

        Fixture fixture = new Fixture();

        TaskRecord<String> record =
                fixture.enqueue(3L, false, 0L);

        fixture.clock.set(1_000_000L);

        assertEquals(0, fixture.manager.sweep());

        assertEquals(TaskStatus.QUEUED, record.status());
        assertEquals(1, fixture.queue.size());
        assertEquals(1, fixture.gate.held());
    }

    @Test
    void doesNotExpireTaskAlreadyCancelled() {

        Fixture fixture = new Fixture();

        TaskRecord<String> record =
                fixture.enqueue(4L, true, 200L);

        assertTrue(record.cancel());

        fixture.clock.set(300L);

        assertEquals(0, fixture.manager.sweep());

        assertEquals(TaskStatus.CANCELLED, record.status());

        // The manager must not release capacity for a task
        // whose cancellation transition already won.
        assertEquals(1, fixture.queue.size());
        assertEquals(1, fixture.gate.held());
    }

    @Test
    void doesNotExpireTaskThatIsNoLongerQueued() {

        Fixture fixture = new Fixture();

        TaskRecord<String> record =
                fixture.enqueue(5L, true, 200L);

        assertTrue(record.tryTransition(TaskStatus.SCHEDULED));

        fixture.clock.set(300L);

        assertEquals(0, fixture.manager.sweep());

        assertEquals(TaskStatus.SCHEDULED, record.status());
        assertEquals(1, fixture.queue.size());
        assertEquals(1, fixture.gate.held());
    }
}
