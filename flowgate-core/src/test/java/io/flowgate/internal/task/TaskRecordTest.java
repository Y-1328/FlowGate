
package io.flowgate.internal.task;

import io.flowgate.api.TaskStatus;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CancellationException;

import static org.junit.jupiter.api.Assertions.*;

class TaskRecordTest {

    private TaskRecord<String> createRecord() {
        return new TaskRecord<>(
                1L,
                "normal",
                context -> "completed",
                100L,
                false,
                0L
        );
    }

    @Test
    void shouldUseStateMachineAsAuthoritativeState() {
        TaskRecord<String> record = createRecord();

        assertEquals(TaskStatus.CREATED, record.status());

        assertTrue(record.markQueued());

        assertEquals(TaskStatus.QUEUED, record.status());
    }

    @Test
    void shouldNotAllowTaskToBeQueuedTwice() {
        TaskRecord<String> record = createRecord();

        assertTrue(record.markQueued());
        assertFalse(record.markQueued());
    }


    @Test
    void cancellationShouldMakeTaskTerminal() {
        TaskRecord<String> record = createRecord();

        assertTrue(record.markQueued());
        assertTrue(record.cancel());

        assertEquals(TaskStatus.CANCELLED, record.status());
        assertTrue(record.isCancellationRequested());

        java.util.concurrent.CompletionException exception =
                assertThrows(
                        java.util.concurrent.CompletionException.class,
                        () -> record.future().join()
                );

        assertInstanceOf(
                java.util.concurrent.CancellationException.class,
                exception.getCause()
        );
    }


    @Test
    void shouldNotCancelCompletedTask() {
        TaskRecord<String> record = createRecord();

        assertTrue(record.markQueued());
        assertTrue(record.tryTransition(TaskStatus.SCHEDULED));
        assertTrue(record.tryTransition(TaskStatus.RUNNING));
        assertTrue(record.tryTransition(TaskStatus.COMPLETED));

        assertFalse(record.cancel());
        assertEquals(TaskStatus.COMPLETED, record.status());
    }
}
