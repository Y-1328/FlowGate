# FlowGate Competitive Benchmark Suite (initial runnable increment)

This module is a separate evaluation harness. It does not modify FlowGate production behavior and adds no benchmark dependency to `flowgate-core`.

## What runs today

- **FlowGate adapter:** calls the actual `io.flowgate.api.FlowGate` public API from the included source module.
- **JDK adapter:** uses a bounded `ThreadPoolExecutor` with `ArrayBlockingQueue` and `AbortPolicy`.
- **Ten scenario definitions:** SIM-01 through SIM-10 are catalogued. SIM-09 cancellation is explicitly skipped until a comparable cancellation contract is implemented in both adapters.
- **Fixed workload definitions:** task counts, worker limits, queue capacities, inter-arrival intervals, execution durations and deterministic failure injection are specified in `ScenarioCatalog`.
- **Reporting:** Python produces raw combined events, scenario metrics, JSON, CSV, Markdown, HTML and tracking charts.

## Run on Windows

Prerequisites: JDK 17+, Maven 3.9+, Python 3.10+.

```bat
run-benchmarks.bat
```

Or from the repository root:

```bat
mvn -pl flowgate-simulation -am clean install
cd flowgate-simulation
mvn exec:java -Dexec.args="../benchmark-results/latest"
py -m pip install -r requirements.txt
py scripts/generate_report.py --input ../benchmark-results/latest --output ../benchmark-results/latest/report
```

## Run on Linux/macOS

```bash
./flowgate-simulation/run-benchmarks.sh
```

## Outputs

`benchmark-results/latest/` contains one raw CSV per scenario/adapter and a `manifest.json`. The reporting step writes:

- `report/report.html` — browsable summary and charts
- `report/report.md` — methodology and limitations
- `report/metrics.json` — machine-readable metrics
- `report/scenario_metrics.csv` — comparison table
- `report/combined_events.csv` — normalized event ledger
- `report/charts/*.png` — throughput, P95 latency, rejection, latency distribution and outcomes

## Important interpretation limits

This is a **real JVM workload replay harness**, not a deterministic virtual-time simulator. Workload definitions and task-class/failure assignment are fixed, but OS scheduling, JVM warmup and wall-clock measurements vary between runs. Run at least 5 measured repetitions after warmup before publishing results.

Only FlowGate and bounded JDK ThreadPoolExecutor are currently executable adapters. Resilience4j, Netflix concurrency-limits, Failsafe and Reactor are listed in the competitive plan but are not yet integrated. Do not claim their results until adapters invoke their actual libraries.

SIM-09 cancellation is explicitly skipped. CPU, heap, allocation rate, GC, thread count, weighted fairness and recovery-time metrics are not yet collected. The report marks these as limitations rather than inventing values. SIM-04/SIM-05 currently provide differentiated task classes and FlowGate queue policies, but full fairness scoring requires tenant/service-level accounting and equivalent policy configurations in each applicable baseline.

## Methodology requirements before public claims

1. Use the same machine, JDK build, JVM flags and power profile.
2. Perform warmup runs and at least five measured repetitions.
3. Preserve the `manifest.json`, raw event CSVs and exact Git commit for every run.
4. Compare only equivalent semantics. Mark native, composed, unsupported or not applicable capabilities.
5. Add JMH microbenchmarks for isolated overhead; do not treat this scenario runner as a replacement for JMH.
6. Add JFR/OS resource sampling before reporting CPU/memory/GC comparisons.
7. Never tune FlowGate or a competitor only for one scenario without disclosing configuration.
