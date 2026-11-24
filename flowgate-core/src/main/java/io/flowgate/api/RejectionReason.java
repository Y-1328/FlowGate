package io.flowgate.api;

/** Why a submission was refused. Rejections are load/lifecycle conditions, never exceptions. */
public enum RejectionReason {
    /** The target queue is at its configured capacity. */
    QUEUE_FULL,
    /** The global limit on queued tasks across all queues is reached. */
    GLOBAL_LIMIT,
    /** The submission's options are semantically invalid (e.g. a zero or negative deadline). */
    INVALID_TASK,
    /** FlowGate has stopped admitting work. */
    SHUTDOWN,
    /** Reserved for admission policies such as queue-delay shedding (not produced in M1). */
    POLICY
}
