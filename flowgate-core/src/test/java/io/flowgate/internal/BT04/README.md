# BT-04 — Scheduler Fairness & Starvation Abuse

Eight deterministic adversarial JUnit 5 tests targeting the actual FlowGate `DeficitRoundRobinScheduler` API.

## Test files
- `BT04_1HeavyVsLightWorkloadTest`: 4:1 weighted distribution sanity band.
- `BT04_2LowWeightProgressTest`: sustained demand and low-weight progress.
- `BT04_3ContinuousHighTrafficTest`: high-quantum traffic while peers remain eligible.
- `BT04_4QueueMonopolizationTest`: multi-queue progress and no total monopoly.
- `BT04_5BurstRecoveryTest`: queue drains and refills.
- `BT04_6DynamicEligibilityTest`: eligibility changes.
- `BT04_7LongRunFairnessDriftTest`: long-run 5:3:1 ratio tolerance.
- `BT04_8RandomizedSchedulingAbuseTest`: 100k seeded eligibility/depth states.

## Placement
Copy all Java files into:
`flowgate-core/src/test/java/io/flowgate/internal/dispatch/`

## Run individually
Right-click each test in IntelliJ and Run. After all pass, run `mvn clean verify` from the Maven reactor root.

## Scope
Tests only. No production changes. These are scheduler-focused tests and do not replace dispatcher/runtime fairness integration tests. Distribution tolerances are sanity bounds for this scheduler's documented quantum semantics, not latency guarantees or a proof of starvation-free behavior.
