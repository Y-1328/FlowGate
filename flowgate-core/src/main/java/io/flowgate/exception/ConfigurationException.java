package io.flowgate.exception;

import java.util.List;

public final class ConfigurationException extends RuntimeException {

    private final List<String> violations;

    public ConfigurationException(List<String> violations) {
        super("Invalid FlowGate configuration: " + String.join("; ", violations));
        this.violations = List.copyOf(violations);
    }

    public List<String> violations() {
        return violations;
    }
}