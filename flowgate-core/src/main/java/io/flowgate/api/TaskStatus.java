
package io.flowgate.api;

public enum TaskStatus {

    CREATED,
    QUEUED,
    SCHEDULED,
    RUNNING,
    RETRY_WAIT,

    COMPLETED,
    FAILED,
    CANCELLED,
    EXPIRED,
    REJECTED;

    public boolean isTerminal() {
        return switch (this) {
            case COMPLETED, FAILED, CANCELLED,
                 EXPIRED, REJECTED -> true;
            default -> false;
        };
    }
}
