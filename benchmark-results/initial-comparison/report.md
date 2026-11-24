# FlowGate Competitive Benchmark Report

Generated: 2026-11-24T11:33:39.209850+00:00

## Scope and limitations

This is the initial runnable benchmark harness. It currently executes the real FlowGate public API and a bounded JDK `ThreadPoolExecutor` baseline. The other competitor adapters (Resilience4j, Netflix concurrency-limits, Failsafe and Project Reactor) are not wired into this build yet; they must not be represented as measured. SIM-09 cancellation pressure is explicitly skipped pending a common cancellation contract.

The workloads use fixed scenario definitions and deterministic task-class/failure assignment. Arrival timestamps and JVM scheduling are real measurements, so repeated runs will vary. This is not a virtual-time simulator.

## Results

| Scenario | Adapter | Completed | Rejected | Failed | Throughput (tasks/s) | P95 latency (ms) | Rejection rate |
|---|---|---:|---:|---:|---:|---:|---:|
| SIM-01 | flowgate | 1200 | 0 | 0 | 74.08 | 16.287 | 0.00% |
| SIM-01 | jdk-thread-pool | 1200 | 0 | 0 | 75.75 | 16.218 | 0.00% |
| SIM-02 | flowgate | 1527 | 73 | 0 | 88.03 | 301.906 | 4.56% |
| SIM-02 | jdk-thread-pool | 1527 | 73 | 0 | 88.10 | 310.298 | 4.56% |
| SIM-03 | flowgate | 195 | 2205 | 0 | 424.28 | 438.071 | 91.88% |
| SIM-03 | jdk-thread-pool | 228 | 2172 | 0 | 560.74 | 364.968 | 90.50% |
| SIM-04 | flowgate | 1500 | 0 | 0 | 71.12 | 16.341 | 0.00% |
| SIM-04 | jdk-thread-pool | 1500 | 0 | 0 | 76.34 | 16.569 | 0.00% |
| SIM-05 | flowgate | 1500 | 0 | 0 | 73.08 | 16.100 | 0.00% |
| SIM-05 | jdk-thread-pool | 1500 | 0 | 0 | 71.56 | 16.112 | 0.00% |
| SIM-06 | flowgate | 900 | 0 | 0 | 70.62 | 16.542 | 0.00% |
| SIM-06 | jdk-thread-pool | 900 | 0 | 0 | 74.57 | 16.505 | 0.00% |
| SIM-07 | flowgate | 1090 | 0 | 110 | 63.88 | 16.103 | 0.00% |
| SIM-07 | jdk-thread-pool | 1090 | 0 | 110 | 63.98 | 16.076 | 0.00% |
| SIM-08 | flowgate | 248 | 1552 | 0 | 558.65 | 414.979 | 86.22% |
| SIM-08 | jdk-thread-pool | 252 | 1548 | 0 | 547.02 | 442.999 | 86.00% |
| SIM-10 | flowgate | 5000 | 0 | 0 | 70.23 | 16.130 | 0.00% |
| SIM-10 | jdk-thread-pool | 5000 | 0 | 0 | 70.06 | 16.413 | 0.00% |

## Charts

- `charts/throughput_by_scenario.png`
- `charts/p95_latency_tracking.png`
- `charts/rejection_rate_tracking.png`
- `charts/latency_distribution.png`
- `charts/task_outcomes.png`
- Per-scenario queue-depth, worker-utilization and arrivals/completions tracking charts

## Measurement exclusions

CPU, heap, allocation rate, GC, thread count, fairness score, and recovery time are not yet instrumented in this first runnable increment. They are not fabricated or inferred. Add JFR/OS sampling and richer task-class accounting before using those metrics in a public competitive claim.
