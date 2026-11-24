# FlowGate BT-02 — Concurrency Ledger Abuse (Executable Tests)

These JUnit 5 tests target the actual `io.flowgate.internal.ledger.ConcurrencyLedger` and `ExecutionPermit` API from the supplied source.

## Included attacks
- BT-02.1 Concurrent acquisition pressure (128 contenders)
- BT-02.2 Concurrent release pressure (32 permits closed together)
- BT-02.3 Acquire/close race with seeded randomized scheduling
- BT-02.4 100-thread repeated close attack against one permit
- BT-02.5 Reservation-protection contention / borrowing check
- BT-02.6 Seeded randomized acquire-release soak (40 workers × 50,000 operations)

## Installation
Extract `src/test/java/io/flowgate/internal/ledger/` into the matching test source directory of `flowgate-core`.

## Run individually from the Maven reactor root
```powershell
mvn -pl flowgate-core -am -Dtest=BT02_1ConcurrentAcquirePressureTest test
mvn -pl flowgate-core -am -Dtest=BT02_2ConcurrentReleasePressureTest test
mvn -pl flowgate-core -am -Dtest=BT02_3AcquireReleaseRaceTest test
mvn -pl flowgate-core -am -Dtest=BT02_4DoubleReleaseAttackTest test
mvn -pl flowgate-core -am -Dtest=BT02_5ReservationBorrowingContentionTest test
mvn -pl flowgate-core -am -Dtest=BT02_6LongRunningRandomizedLedgerTest test
```
Run BT-02.1 first. Do not treat passing test results as verified until they are executed against your current checkout.

## Safety / scope
- Tests only; no production files are changed.
- All thread pools are fixed-size and terminated in `finally` blocks.
- Randomized tests use fixed seeds for reproducibility.
- The long randomized test has a bounded operation count.
- This package was generated against the uploaded source API, but has not been run in your local Maven project in this environment.
