package io.flowgate.benchmark.model;

public record Scenario(String id, String name, int tasks, int workers, int queueCapacity,
                       long arrivalIntervalMicros, long executionMicros, String pattern,
                       long deadlineMicros) { }
