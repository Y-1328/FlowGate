# BT-03 — Queue Contention & Capacity Abuse

Drop the Java test files into the existing FlowGate package:
`flowgate-core/src/test/java/io/flowgate/internal/queue/`

No production code is included or modified. The tests target the queue and global admission gate directly.

## Tests
- BT03.1 Queue capacity flood
- BT03.2 Multi-queue saturation
- BT03.3 Concurrent admission boundary
- BT03.4 Drain/refill producer-consumer race
- BT03.5 Rejection pressure
- BT03.6 Local capacity isolation with shared global bound
- BT03.7 Repeated saturation cycles
- BT03.8 Seeded randomized submit/poll/remove abuse

Run from the FlowGate reactor root:

```powershell
mvn -pl flowgate-core -Dtest=BT03_1QueueCapacityFloodTest test
mvn -pl flowgate-core -Dtest=BT03_2MultiQueueSaturationTest test
mvn -pl flowgate-core -Dtest=BT03_3ConcurrentAdmissionBoundaryTest test
mvn -pl flowgate-core -Dtest=BT03_4DrainRefillRaceTest test
mvn -pl flowgate-core -Dtest=BT03_5RejectionPressureTest test
mvn -pl flowgate-core -Dtest=BT03_6CapacityIsolationTest test
mvn -pl flowgate-core -Dtest=BT03_7RepeatedSaturationCyclesTest test
mvn -pl flowgate-core -Dtest=BT03_8RandomizedQueueAbuseTest test
```

Note: This package was generated against the supplied source signatures but has not yet been run in the user's local Maven environment. Validate individually before drawing conclusions.
