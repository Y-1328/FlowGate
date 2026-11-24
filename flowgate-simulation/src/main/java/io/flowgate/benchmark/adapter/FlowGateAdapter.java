package io.flowgate.benchmark.adapter;

import io.flowgate.api.*;
import io.flowgate.benchmark.model.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

/** Adapter using the uploaded FlowGate public API, not a reimplementation. */
public final class FlowGateAdapter implements BenchmarkAdapter {
    @Override public String id() { return "flowgate"; }
    @Override public String displayName() { return "FlowGate"; }

    @Override public List<TaskEvent> execute(Scenario s) throws Exception {
        boolean usesThreeQueues = s.pattern().equals("unequal") || s.pattern().equals("priority");
        int perQueueCapacity = usesThreeQueues ? Math.max(1, (int)Math.ceil(s.queueCapacity() / 3.0)) : s.queueCapacity();
        int inactiveQueueCapacity = 1;
        FlowGate gate = FlowGate.builder().globalConcurrency(s.workers())
            .queue("critical", q -> q.priority(Priority.CRITICAL).weight(4).capacity(usesThreeQueues ? perQueueCapacity : inactiveQueueCapacity).maxConcurrency(s.workers()))
            .queue("normal", q -> q.priority(Priority.NORMAL).weight(2).capacity(perQueueCapacity).maxConcurrency(s.workers()))
            .queue("bulk", q -> q.priority(Priority.LOW).weight(1).capacity(usesThreeQueues ? perQueueCapacity : inactiveQueueCapacity).maxConcurrency(s.workers()))
            .build();
        List<TaskEvent> events = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch done = new CountDownLatch(s.tasks());
        gate.start();
        try {
            for (int i = 0; i < s.tasks(); i++) {
                long arrival = System.nanoTime();
                String workload = workloadFor(s, i);
                String queue = queueFor(workload);
                long deadline = s.deadlineMicros() > 0 ? arrival + TimeUnit.MICROSECONDS.toNanos(s.deadlineMicros()) : 0L;
                long taskId = i;
                Submission<Void> submission = gate.submit(queue, context -> {
                    long started = System.nanoTime();
                    LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(executionFor(s, taskId)));
                    if (s.pattern().equals("failure") && taskId % 11 == 0) throw new IllegalStateException("deterministic injected failure");
                    long finished = System.nanoTime();
                    events.add(new TaskEvent(taskId, id(), s.id(), workload, arrival, started, finished,
                        deadline > 0 && finished > deadline ? "COMPLETED_LATE" : "COMPLETED", deadline));
                    done.countDown();
                    return null;
                });
                if (submission.isRejected()) {
                    events.add(new TaskEvent(taskId, id(), s.id(), workload, arrival, 0, 0,
                        "REJECTED_" + submission.rejectionReason(), deadline));
                    done.countDown();
                } else {
                    submission.future().whenComplete((v, ex) -> {
                        if (ex != null) {
                            events.add(new TaskEvent(taskId, id(), s.id(), workload, arrival, 0, System.nanoTime(), "FAILED", deadline));
                            done.countDown();
                        }
                    });
                }
                pauseArrival(s, i);
            }
            if (!done.await(5, TimeUnit.MINUTES)) throw new TimeoutException("FlowGate scenario timed out: " + s.id());
        } finally { gate.close(); }
        return sorted(events);
    }

    public static String workloadFor(Scenario s, int i) {
        return switch (s.pattern()) {
            case "unequal" -> i % 10 < 2 ? "critical" : i % 10 < 5 ? "normal" : "bulk";
            case "priority" -> i % 3 == 0 ? "critical" : i % 3 == 1 ? "normal" : "bulk";
            case "tenant" -> i % 3 == 0 ? "tenant-a" : i % 3 == 1 ? "tenant-b" : "tenant-c";
            default -> "normal";
        };
    }
    public static String queueFor(String workload) {
        if (workload.equals("critical")) return "critical";
        if (workload.equals("bulk")) return "bulk";
        return "normal";
    }
    public static long executionFor(Scenario s, long i) {
        if (s.pattern().equals("slow")) return s.executionMicros() * (i % 5 == 0 ? 8 : 1);
        if (s.pattern().equals("unequal")) return s.executionMicros() * (i % 10 < 2 ? 1 : i % 10 < 5 ? 2 : 4);
        if (s.pattern().equals("failure")) return s.executionMicros();
        return s.executionMicros();
    }
    public static void pauseArrival(Scenario s, int i) {
        long interval = s.arrivalIntervalMicros();
        if (s.pattern().equals("burst") && i < s.tasks() / 5) interval = 0;
        if (s.pattern().equals("overload") && i % 5 != 0) interval = 0;
        if (s.pattern().equals("recovery") && i < s.tasks() / 2) interval = 0;
        if (interval > 0) LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(interval));
    }
    public static List<TaskEvent> sorted(List<TaskEvent> events) {
        List<TaskEvent> copy = new ArrayList<>(events);
        copy.sort(Comparator.comparingLong(TaskEvent::taskId));
        return copy;
    }
}
