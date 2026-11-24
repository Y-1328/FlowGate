package io.flowgate.internal.BT08;

import io.flowgate.api.Priority;
import io.flowgate.config.FlowGateConfig;
import io.flowgate.config.QueueConfig;
import io.flowgate.internal.runtime.FlowGateRuntime;
import io.flowgate.policy.RetryPolicy;

import java.time.Duration;
import java.util.List;

final class BT08TestSupport {

    private BT08TestSupport() {
    }

    static QueueConfig queue(
            String id,
            int capacity,
            int maxConcurrency,
            RetryPolicy retryPolicy) {

        return new QueueConfig(
                id,
                Priority.NORMAL,
                1,
                capacity,
                0,
                maxConcurrency,
                null,
                retryPolicy
        );
    }

    static FlowGateRuntime runtime(
            int globalConcurrency,
            QueueConfig... queues) {

        FlowGateConfig config = new FlowGateConfig(
                globalConcurrency,
                List.of(queues)
        );

        return new FlowGateRuntime(
                config,
                Duration.ofMillis(2)
        );
    }
}