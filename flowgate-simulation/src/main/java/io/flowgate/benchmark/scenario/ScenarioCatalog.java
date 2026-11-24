package io.flowgate.benchmark.scenario;

import io.flowgate.benchmark.model.Scenario;
import java.util.List;

public final class ScenarioCatalog {
    private ScenarioCatalog() { }
    public static List<Scenario> all() {
        return List.of(
            new Scenario("SIM-01", "baseline-traffic", 1200, 8, 300, 250, 500, "baseline", 0),
            new Scenario("SIM-02", "sudden-burst", 1600, 8, 240, 80, 700, "burst", 0),
            new Scenario("SIM-03", "sustained-overload", 2400, 8, 180, 0, 1200, "overload", 0),
            new Scenario("SIM-04", "unequal-service-workloads", 1500, 8, 360, 120, 500, "unequal", 0),
            new Scenario("SIM-05", "priority-contention", 1500, 8, 360, 100, 600, "priority", 0),
            new Scenario("SIM-06", "slow-execution", 900, 6, 240, 150, 1500, "slow", 0),
            new Scenario("SIM-07", "failure-injection", 1200, 8, 300, 150, 600, "failure", 0),
            new Scenario("SIM-08", "overload-recovery", 1800, 8, 240, 0, 800, "recovery", 0),
            new Scenario("SIM-09", "cancellation-pressure", 600, 6, 180, 100, 1000, "cancellation", 0),
            new Scenario("SIM-10", "long-duration-endurance", 5000, 8, 1000, 80, 250, "endurance", 0)
        );
    }
}
