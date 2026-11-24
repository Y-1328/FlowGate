package io.flowgate.internal.admission;

/** Outcome of offering a task in RETRY_WAIT back to normal admission. */
public enum ReadmissionResult {

    /** The same task record was published to its queue; it is QUEUED again. */
    ADMITTED,

    /** The task was no longer in RETRY_WAIT (for example it was cancelled); nothing was changed. */
    NOT_WAITING,

    /** The original deadline has elapsed; nothing was reserved. The caller must expire the task. */
    DEADLINE_EXPIRED,

    /** The target queue is at capacity. Nothing was reserved. */
    QUEUE_FULL,

    /** The global admission limit is reached. Nothing was reserved. */
    GLOBAL_LIMIT,

    /** Admission is closed. Nothing was reserved. */
    SHUTDOWN
}
