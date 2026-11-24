package io.flowgate.benchmark.adapter;

import io.flowgate.benchmark.model.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

/** Bounded ThreadPoolExecutor baseline with AbortPolicy. */
public final class JdkThreadPoolAdapter implements BenchmarkAdapter {
    @Override public String id() { return "jdk-thread-pool"; }
    @Override public String displayName() { return "JDK ThreadPoolExecutor"; }

    @Override public List<TaskEvent> execute(Scenario s) throws Exception {
        ThreadPoolExecutor pool = new ThreadPoolExecutor(s.workers(), s.workers(), 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(Math.max(1, s.queueCapacity())), r -> {
                    Thread t = new Thread(r, "jdk-benchmark-worker"); t.setDaemon(false); return t;
                }, new ThreadPoolExecutor.AbortPolicy());
        List<TaskEvent> events = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch done = new CountDownLatch(s.tasks());
        try {
            for (int i = 0; i < s.tasks(); i++) {
                final int taskId = i;
                final long arrival = System.nanoTime();
                final String workload = FlowGateAdapter.workloadFor(s, i);
                final long deadline = s.deadlineMicros() > 0 ? arrival + TimeUnit.MICROSECONDS.toNanos(s.deadlineMicros()) : 0L;
                try {
                    pool.execute(() -> {
                        long started = System.nanoTime();
                        try {
                            LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(FlowGateAdapter.executionFor(s, taskId)));
                            if (s.pattern().equals("failure") && taskId % 11 == 0) throw new IllegalStateException("deterministic injected failure");
                            long finished = System.nanoTime();
                            events.add(new TaskEvent(taskId, id(), s.id(), workload, arrival, started, finished,
                                    deadline > 0 && finished > deadline ? "COMPLETED_LATE" : "COMPLETED", deadline));
                        } catch (Throwable ex) {
                            events.add(new TaskEvent(taskId, id(), s.id(), workload, arrival, started, System.nanoTime(), "FAILED", deadline));
                        } finally { done.countDown(); }
                    });
                } catch (RejectedExecutionException ex) {
                    events.add(new TaskEvent(taskId, id(), s.id(), workload, arrival, 0, 0, "REJECTED", deadline));
                    done.countDown();
                }
                FlowGateAdapter.pauseArrival(s, i);
            }
            if (!done.await(5, TimeUnit.MINUTES)) throw new TimeoutException("JDK scenario timed out: " + s.id());
        } finally {
            pool.shutdown();
            if (!pool.awaitTermination(30, TimeUnit.SECONDS)) pool.shutdownNow();
        }
        return FlowGateAdapter.sorted(events);
    }
}
