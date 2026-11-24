package io.flowgate.api;

/** A unit of work submitted to a FlowGate queue. */
@FunctionalInterface
public interface WorkloadTask<T> {
    T run(TaskContext context) throws Exception;
}
