package io.flowgate.internal.BT05;

import io.flowgate.api.Priority;
import io.flowgate.config.FlowGateConfig;
import io.flowgate.config.QueueConfig;
import io.flowgate.internal.runtime.FlowGateRuntime;

import java.time.Duration;
import java.util.List;

final class BT05TestSupport {

    static final Duration TIMEOUT = Duration.ofSeconds(10);

    private BT05TestSupport() {}

    static FlowGateRuntime runtime(
            int globalConcurrency,
            int queueCapacity,
            int queueConcurrency) {

        QueueConfig queue = new QueueConfig(
                "default",
                Priority.NORMAL,
                1,
                queueCapacity,
                1,                  // reservedConcurrency
                queueConcurrency,     // maxConcurrency
                null
        );

        return new FlowGateRuntime(
                new FlowGateConfig(globalConcurrency, List.of(queue)),
                Duration.ofMillis(5)
        );
    }
}