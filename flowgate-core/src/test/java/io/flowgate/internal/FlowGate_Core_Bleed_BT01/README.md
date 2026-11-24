# FlowGate Core Bleed Testing — BT-01


This is a **test-only adversarial suite** for the FlowGate core module. It is designed to pressure internal concurrency primitives directly. It does not modify production code.


## Included attacks
- **BT-01A GlobalAdmissionGate** — acquisition contention and close/acquire race.
- **BT-01B WorkloadQueue** — concurrent slot reservations and reservation rollback.
- **BT-01C ConcurrencyLedger** — simultaneous permit acquisition/release, global bound, idempotent permit close.
- **BT-01D TaskStateMachine** — CAS transition race and terminal-state resistance.
- **BT-01E DRR Scheduler** — deterministic randomized churn of eligible/waiting/quantum arrays.


## Installation
Copy the package folders from `src/test/java/io/flowgate/internal/` into the matching `flowgate-core/src/test/java/io/flowgate/internal/` directory. Keep these tests separate from normal tests if you want a dedicated bleed profile/folder.


## Run
From the `flowgate-core` module root:


```bash
mvn -Dtest='*BleedTest' test
```

To run one attack:


```bash
mvn -Dtest=GlobalAdmissionGateBleedTest test
mvn -Dtest=WorkloadQueueBleedTest test
mvn -Dtest=ConcurrencyLedgerBleedTest test
mvn -Dtest=TaskStateMachineBleedTest test
mvn -Dtest=DeficitRoundRobinBleedTest test
```


## Important interpretation notes
- These tests intentionally use high thread counts and long iteration counts. Run them one at a time initially.
- Assertion failures may expose defects; do not immediately alter production code. Preserve the failing seed/output and investigate.
- BT-01B tests reservation/rollback pressure, not published queue elements. Queue publication/poll races are a later attack.
- BT-01E uses a fixed seed for reproducibility and is not a performance benchmark.
- The queue test's `size()` is the reservation counter by design, so transient reservations are included.
- This initial suite is a first wave, not a complete audit. BT-02 will separately test publication/poll/remove races and reservation ownership failure modes.
