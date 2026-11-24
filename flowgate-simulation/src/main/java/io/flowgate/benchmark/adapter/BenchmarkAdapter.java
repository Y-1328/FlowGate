package io.flowgate.benchmark.adapter;

import io.flowgate.benchmark.model.Scenario;
import io.flowgate.benchmark.model.TaskEvent;
import java.util.List;

public interface BenchmarkAdapter {
    String id();
    String displayName();
    List<TaskEvent> execute(Scenario scenario) throws Exception;
}
