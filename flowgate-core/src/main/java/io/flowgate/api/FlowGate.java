package io.flowgate.api;

import io.flowgate.config.FlowGateConfig;
import io.flowgate.config.QueueConfig;
import io.flowgate.internal.runtime.FlowGateRuntime;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public final class FlowGate implements AutoCloseable {

    private final FlowGateRuntime runtime;
    private final AtomicBoolean startCalled = new AtomicBoolean(false);

    private FlowGate(FlowGateConfig config, Duration idleWait) {
        this.runtime = new FlowGateRuntime(config, idleWait);
    }

    public static Builder builder() {
        return new Builder();
    }

    // ---------------- Lifecycle ----------------

    public void start() {
        if (!startCalled.compareAndSet(false, true)) {
            throw new IllegalStateException(
                    "FlowGate has already been started"
            );
        }

        try {
            runtime.start();
        } catch (RuntimeException | Error e) {
            startCalled.set(false);
            throw e;
        }
    }

    public boolean isRunning() {
        return runtime.isRunning();
    }

    @Override
    public void close() {
        runtime.close();
    }

    // ---------------- Task Submission ----------------

    public Submission<Void> submit(String queue, Runnable task) {
        Objects.requireNonNull(task, "task");

        return runtime.submit(queue, context -> {
            task.run();
            return null;
        });
    }

    public Submission<Void> submit(
            String queue,
            Runnable task,
            Duration deadline
    ) {
        Objects.requireNonNull(task, "task");

        return runtime.submit(queue, context -> {
            task.run();
            return null;
        }, deadline);
    }

    public <T> Submission<T> submit(
            String queue,
            WorkloadTask<T> task
    ) {
        return runtime.submit(
                queue,
                Objects.requireNonNull(task, "task")
        );
    }

    public <T> Submission<T> submit(
            String queue,
            WorkloadTask<T> task,
            Duration deadline
    ) {
        return runtime.submit(
                queue,
                Objects.requireNonNull(task, "task"),
                deadline
        );
    }

    // ---------------- Builder ----------------

    public static final class Builder {

        private int globalConcurrency = -1;
        private Duration idleWait = Duration.ofMillis(1);

        private final List<QueueBuilder> queues = new ArrayList<>();

        private Builder() {}

        public Builder globalConcurrency(int concurrency) {
            this.globalConcurrency = concurrency;
            return this;
        }

        public Builder idleWait(Duration idleWait) {
            this.idleWait = Objects.requireNonNull(
                    idleWait,
                    "idleWait"
            );
            return this;
        }

        public Builder queue(
                String id,
                Consumer<QueueBuilder> configure
        ) {
            Objects.requireNonNull(configure, "configure");

            QueueBuilder queue = new QueueBuilder(id);
            configure.accept(queue);
            queues.add(queue);

            return this;
        }

        public FlowGate build() {
            validate();

            Set<String> ids = new HashSet<>();
            List<QueueConfig> resolved = new ArrayList<>();
            long totalReserved = 0;

            for (QueueBuilder builder : queues) {
                QueueConfig config = builder.build(globalConcurrency);

                // Validate queue ID format
                if (!config.id().matches("[A-Za-z0-9._-]{1,64}")) {
                    throw new IllegalArgumentException(
                            "Invalid queue ID: " + config.id()
                    );
                }

                // Check duplicate queue IDs
                if (!ids.add(config.id())) {
                    throw new IllegalArgumentException(
                            "Duplicate queue ID: " + config.id()
                    );
                }

                // Validate queue concurrency
                if (config.maxConcurrency() > globalConcurrency) {
                    throw new IllegalArgumentException(
                            "Queue maxConcurrency cannot exceed globalConcurrency"
                    );
                }

                totalReserved += config.reservedConcurrency();
                resolved.add(config);
            }

            // Validate total reserved concurrency
            if (totalReserved > globalConcurrency) {
                throw new IllegalArgumentException(
                        "Total reserved concurrency cannot exceed globalConcurrency"
                );
            }

            FlowGateConfig config = new FlowGateConfig(
                    globalConcurrency,
                    List.copyOf(resolved)
            );

            return new FlowGate(config, idleWait);
        }

        private void validate() {
            if (globalConcurrency < 1) {
                throw new IllegalArgumentException(
                        "globalConcurrency must be at least 1"
                );
            }

            if (idleWait.isZero() || idleWait.isNegative()) {
                throw new IllegalArgumentException(
                        "idleWait must be positive"
                );
            }

            if (queues.isEmpty()) {
                throw new IllegalArgumentException(
                        "At least one queue must be configured"
                );
            }
        }
    }
}