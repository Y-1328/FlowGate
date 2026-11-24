package io.flowgate.api;

/** Whether a submission entered FlowGate. Details of a refusal are in {@link RejectionReason}. */
public enum AdmissionOutcome {
    ACCEPTED,
    REJECTED
}
