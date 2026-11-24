
package io.flowgate.config;


import io.flowgate.exception.ConfigurationException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


final class ConfigValidator {

    private ConfigValidator() {
        throw new UnsupportedOperationException(
                "Utility class cannot be instantiated"
        );
    }

    /**
     * Validates the complete FlowGate configuration.
     *
     * @return queues keyed by ID, preserving configuration order
     * @throws ConfigurationException if any violations are found
     */
    static Map<String, QueueConfig> validate(
            int globalConcurrency,
            List<QueueConfig> queues
    ) {
        List<String> errors = new ArrayList<>();
        Map<String, QueueConfig> queuesById = new LinkedHashMap<>();

        long reservedSum = 0;

        if (globalConcurrency < 1) {
            errors.add("globalConcurrency must be >= 1");
        }

        if (queues == null) {
            errors.add("queues must not be null");
        } else if (queues.isEmpty()) {
            errors.add("at least one queue is required");
        }

        if (queues != null) {
            for (QueueConfig queue : queues) {

                if (queue == null) {
                    errors.add("queue configuration must not be null");
                    continue;
                }

                String id = queue.id();
                String label = String.valueOf(id);

                // Validate queue ID.
                if (id == null ||
                        !QueueConfig.VALID_ID.matcher(id).matches()) {

                    errors.add(
                            "Invalid queue id: '" + id +
                                    "' (allowed: [A-Za-z0-9._-]{1,64})"
                    );

                } else {
                    QueueConfig previous =
                            queuesById.putIfAbsent(id, queue);

                    if (previous != null) {
                        errors.add("Duplicate queue id: " + id);
                    }
                }

                // Validate individual queue settings.
                if (queue.priority() == null) {
                    errors.add(label + ": priority must not be null");
                }

                if (queue.capacity() < 1) {
                    errors.add(label + ": capacity must be >= 1");
                }

                if (queue.weight() < 1) {
                    errors.add(label + ": weight must be >= 1");
                }

                if (queue.reservedConcurrency() < 0) {
                    errors.add(label + ": reserved must be >= 0");
                }

                if (queue.maxConcurrency() < 1) {
                    errors.add(label + ": max must be >= 1");
                }

                if (queue.reservedConcurrency() >= 0 && queue.maxConcurrency() >= 1
                        && queue.reservedConcurrency() > queue.maxConcurrency()) {

                    errors.add(label + ": reserved > max");
                }

                if (globalConcurrency >= 1
                        && queue.maxConcurrency() > globalConcurrency) {

                    errors.add(label + ": max > globalConcurrency");
                }

                if (queue.reservedConcurrency() > 0) {
                    reservedSum += queue.reservedConcurrency();
                }
            }
        }

        // Validate aggregate resource reservation.
        if (reservedSum > globalConcurrency) {
            errors.add(
                    "Sum of reserved (" + reservedSum +
                            ") exceeds globalConcurrency (" +
                            globalConcurrency + ")"
            );
        }

        if (!errors.isEmpty()) {
            throw new ConfigurationException(errors);
        }

        return Collections.unmodifiableMap(queuesById);
    }
}
