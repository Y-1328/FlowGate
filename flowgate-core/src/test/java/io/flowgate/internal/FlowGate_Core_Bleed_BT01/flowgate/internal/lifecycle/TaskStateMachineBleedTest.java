package io.flowgate.internal.FlowGate_Core_Bleed_BT01.flowgate.internal.lifecycle;

import io.flowgate.api.TaskStatus;
import io.flowgate.internal.lifecycle.TaskStateMachine;
import org.junit.jupiter.api.Test;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;
/** BT-01D: CAS transition race and terminal-state resistance. */
class TaskStateMachineBleedTest {
    @Test
    void onlyOneConcurrentCreatedToQueuedTransitionCanWin() throws Exception {
        final int threads = 128;
        TaskStateMachine machine = new TaskStateMachine();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads), start = new CountDownLatch(1);
        AtomicInteger winners = new AtomicInteger();
        try {
            for (int i = 0; i < threads; i++) pool.submit(() -> {
                ready.countDown();
                try { start.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
                if (machine.tryTransition(TaskStatus.QUEUED)) winners.incrementAndGet();
            });
            assertTrue(ready.await(20, TimeUnit.SECONDS));
            start.countDown(); pool.shutdown();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
            assertEquals(1, winners.get());
            assertEquals(TaskStatus.QUEUED, machine.status());
        } finally { start.countDown(); pool.shutdownNow(); }
    }


    @Test
    void terminalStateMustRejectAllFurtherTransitions() {

        for (TaskStatus terminal : new TaskStatus[]{
                TaskStatus.COMPLETED,
                TaskStatus.FAILED,
                TaskStatus.CANCELLED,
                TaskStatus.EXPIRED,
                TaskStatus.REJECTED
        }) {

            TaskStateMachine machine = new TaskStateMachine();

            // Reach a valid path to the requested terminal state.
            switch (terminal) {

                case REJECTED:
                    assertTrue(machine.tryTransition(TaskStatus.REJECTED));
                    break;

                case EXPIRED:
                    assertTrue(machine.tryTransition(TaskStatus.QUEUED));
                    assertTrue(machine.tryTransition(TaskStatus.EXPIRED));
                    break;

                default:
                    assertTrue(machine.tryTransition(TaskStatus.QUEUED));
                    assertTrue(machine.tryTransition(TaskStatus.SCHEDULED));
                    assertTrue(machine.tryTransition(TaskStatus.RUNNING));
                    assertTrue(machine.tryTransition(terminal));
                    break;
            }

            // Verify the terminal state was reached.
            assertEquals(terminal, machine.status());

            // Attack: attempt every possible transition from terminal.
            for (TaskStatus next : TaskStatus.values()) {

                assertFalse(
                        machine.tryTransition(next),
                        "Terminal state " + terminal
                                + " must reject transition to " + next
                );
            }

            // State must remain unchanged after the attack.
            assertEquals(terminal, machine.status());
        }
    }
}
