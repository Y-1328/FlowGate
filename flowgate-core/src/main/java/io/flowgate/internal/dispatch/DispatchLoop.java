
package io.flowgate.internal.dispatch;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.locks.LockSupport;

/**
 * Runs the FlowGate dispatcher on a dedicated thread.
 *
 * Owns only the dispatcher thread, not the worker executor.
 */
public final class DispatchLoop implements AutoCloseable {

    private enum State {
        NEW,
        RUNNING,
        STOPPING,
        TERMINATED
    }

    private final Dispatcher dispatcher;
    private final long idleWaitNanos;

    private volatile State state = State.NEW;
    private volatile Thread dispatcherThread;
    private volatile Throwable failure;

    public DispatchLoop(Dispatcher dispatcher, Duration idleWait) {
        this.dispatcher = Objects.requireNonNull(
                dispatcher, "dispatcher must not be null");

        Objects.requireNonNull(
                idleWait, "idleWait must not be null");

        if (idleWait.isZero() || idleWait.isNegative()) {
            throw new IllegalArgumentException(
                    "Idle wait must be positive");
        }

        try {
            this.idleWaitNanos = idleWait.toNanos();
        } catch (ArithmeticException exception) {
            throw new IllegalArgumentException(
                    "Idle wait is too large", exception);
        }

        if (idleWaitNanos <= 0) {
            throw new IllegalArgumentException(
                    "Idle wait is too small");
        }
    }

    /**
     * Starts the dispatcher exactly once.
     */
    public synchronized void start() {
        if (state != State.NEW) {
            throw new IllegalStateException(
                    "DispatchLoop cannot be started in state " + state);
        }

        state = State.RUNNING;

        Thread thread = new Thread(
                this::runLoop,
                "flowgate-dispatcher"
        );

        dispatcherThread = thread;

        try {
            thread.start();
        } catch (RuntimeException | Error exception) {
            failure = exception;
            state = State.TERMINATED;
            throw exception;
        }
    }

    /**
     * Signals that work may be available.
     */
    public void signal() {
        Thread thread = dispatcherThread;

        if (state == State.RUNNING && thread != null) {
            LockSupport.unpark(thread);
        }
    }

    public boolean isRunning() {
        Thread thread = dispatcherThread;

        return state == State.RUNNING
                && thread != null
                && thread.isAlive();
    }

    /**
     * Returns the unexpected failure that terminated the loop.
     *
     * @return failure, or null if no failure occurred
     */
    public Throwable failure() {
        return failure;
    }

    private void runLoop() {
        try {
            while (state == State.RUNNING) {

                // External interruption requests termination.
                if (Thread.interrupted()) {
                    state = State.STOPPING;
                    break;
                }

                boolean dispatched = false;

                while (state == State.RUNNING) {

                    if (Thread.interrupted()) {
                        state = State.STOPPING;
                        break;
                    }

                    if (!dispatcher.dispatchOne()) {
                        break;
                    }

                    dispatched = true;
                }

                if (state != State.RUNNING) {
                    break;
                }

                if (!dispatched) {
                    LockSupport.parkNanos(this, idleWaitNanos);
                }
            }

        } catch (Throwable exception) {

            // Publish termination before making the failure visible.
            state = State.TERMINATED;
            failure = exception;

            // Preserve fatal JVM errors after recording the failure.
            if (exception instanceof Error error) {
                throw error;
            }

        } finally {
            state = State.TERMINATED;
        }
    }

    /**
     * Permanently stops the dispatch loop.
     *
     * Already dispatched tasks are not interrupted.
     * The worker executor remains owned by the runtime.
     */
    @Override
    public void close() {
        Thread thread;

        synchronized (this) {
            if (state == State.NEW) {
                state = State.TERMINATED;
                return;
            }

            if (state == State.RUNNING) {
                state = State.STOPPING;
            }

            thread = dispatcherThread;
        }

        if (thread == null || thread == Thread.currentThread()) {
            return;
        }

        LockSupport.unpark(thread);

        boolean interrupted = false;

        while (thread.isAlive()) {
            try {
                thread.join();
            } catch (InterruptedException exception) {
                interrupted = true;
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
