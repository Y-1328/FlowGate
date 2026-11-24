
package io.flowgate.internal.lifecycle;

import io.flowgate.api.TaskStatus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TaskStateMachineTest {

    @Test
    void shouldStartInCreatedState() {
        TaskStateMachine machine = new TaskStateMachine();

        assertEquals(TaskStatus.CREATED, machine.status());
    }

    @Test
    void shouldTransitionFromCreatedToQueued() {
        TaskStateMachine machine = new TaskStateMachine();

        assertTrue(machine.tryTransition(TaskStatus.QUEUED));
        assertEquals(TaskStatus.QUEUED, machine.status());
    }

    @Test
    void shouldRejectInvalidTransition() {
        TaskStateMachine machine = new TaskStateMachine();

        assertFalse(machine.tryTransition(TaskStatus.RUNNING));
        assertEquals(TaskStatus.CREATED, machine.status());
    }

    @Test
    void shouldSupportNormalExecutionLifecycle() {
        TaskStateMachine machine = new TaskStateMachine();

        assertTrue(machine.tryTransition(TaskStatus.QUEUED));
        assertTrue(machine.tryTransition(TaskStatus.SCHEDULED));
        assertTrue(machine.tryTransition(TaskStatus.RUNNING));
        assertTrue(machine.tryTransition(TaskStatus.COMPLETED));

        assertEquals(TaskStatus.COMPLETED, machine.status());
    }

    @Test
    void shouldNotAllowTransitionFromTerminalState() {
        TaskStateMachine machine = new TaskStateMachine();

        machine.tryTransition(TaskStatus.QUEUED);
        machine.tryTransition(TaskStatus.CANCELLED);

        assertFalse(machine.tryTransition(TaskStatus.RUNNING));
        assertEquals(TaskStatus.CANCELLED, machine.status());
    }

    @Test
    void scheduledTaskCanFailBeforeWorkerStarts() {
        TaskStateMachine machine = new TaskStateMachine();

        assertTrue(machine.tryTransition(TaskStatus.QUEUED));
        assertTrue(machine.tryTransition(TaskStatus.SCHEDULED));
        assertTrue(machine.tryTransition(TaskStatus.FAILED));

        assertEquals(TaskStatus.FAILED, machine.status());
    }
}
