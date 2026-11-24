package io.flowgate.exception;

/**
 * Completes a task's future when FlowGate stopped retrying it <em>for a reason
 * other than the task itself being non-retryable</em>.
 *
 * <p>The {@linkplain #getCause() cause} is always the failure of the most recent
 * attempt. Earlier attempts' failures are intentionally not retained, so the
 * memory held by a retrying task is bounded regardless of the attempt limit.
 *
 * <p>A failure that the retry policy does not consider retryable (and a failure
 * after the deadline has passed) is <em>not</em> wrapped: the future fails with
 * the task's own exception, exactly as it would without retries.
 */
public final class RetryFailedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /** Why FlowGate gave up retrying. */
    public enum Reason {
        /** The attempt limit was reached; every attempt failed. */
        ATTEMPTS_EXHAUSTED,
        /** The bounded retry-wait capacity was full when the failure occurred. */
        RETRY_CAPACITY_FULL,
        /** The backoff elapsed but normal admission refused the retry (queue or global limit full). */
        READMISSION_REJECTED,
        /** An unexpected internal error prevented scheduling the retry. */
        SCHEDULING_FAILED
    }

    private final Reason reason;
    private final int attempts;

    public RetryFailedException(
            Reason reason,
            int attempts,
            String message,
            Throwable lastFailure
    ) {
        super(message, lastFailure);
        this.reason = reason;
        this.attempts = attempts;
    }

    public Reason reason() {
        return reason;
    }

    /** Number of attempts that actually executed. */
    public int attempts() {
        return attempts;
    }
}
