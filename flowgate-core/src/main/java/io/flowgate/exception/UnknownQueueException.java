package io.flowgate.exception;

/** Thrown when a submission names a queue that was never configured (a programming error). */
public final class UnknownQueueException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    public UnknownQueueException(String queueId) {
        super("Unknown queue id: " + queueId);
    }
}
