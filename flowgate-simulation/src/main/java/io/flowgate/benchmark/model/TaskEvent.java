package io.flowgate.benchmark.model;

public record TaskEvent(long taskId, String adapter, String scenario, String workload,
                        long arrivalNanos, long startNanos, long finishNanos,
                        String status, long deadlineNanos) { }
