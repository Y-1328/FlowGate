package io.flowgate.benchmark;

import io.flowgate.benchmark.adapter.*;
import io.flowgate.benchmark.model.*;
import io.flowgate.benchmark.scenario.ScenarioCatalog;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;

public final class BenchmarkMain {
    private static final String HEADER = "task_id,adapter,scenario,workload,arrival_nanos,start_nanos,finish_nanos,status,deadline_nanos";
    public static void main(String[] args) throws Exception {
        Path output = Path.of(args.length > 0 ? args[0] : "../benchmark-results/latest").toAbsolutePath().normalize();
        Files.createDirectories(output);
        List<BenchmarkAdapter> adapters = List.of(new FlowGateAdapter(), new JdkThreadPoolAdapter());
        List<Scenario> scenarios = ScenarioCatalog.all();
        writeManifest(output, scenarios, adapters);
        for (Scenario scenario : scenarios) {
            writeTrace(output, scenario);
            for (BenchmarkAdapter adapter : adapters) {
                System.out.printf("Running %s / %s ...%n", scenario.id(), adapter.displayName());
                List<TaskEvent> events;
                if (scenario.pattern().equals("cancellation")) {
                    // Explicitly recorded as unsupported until a common cancellation contract is implemented.
                    writeSkipped(output, scenario, adapter, "Cancellation semantics differ; adapter-level cancellation contract not yet implemented.");
                    continue;
                }
                events = adapter.execute(scenario);
                writeEvents(output.resolve(scenario.id() + "_" + adapter.id() + ".csv"), events);
            }
        }
        System.out.println("Raw benchmark event files written to: " + output);
        System.out.println("Next: run the Python reporting pipeline to generate tracking charts and reports.");
    }

    private static void writeTrace(Path output, Scenario s) throws IOException {
        Path path = output.resolve(s.id() + "_workload_trace.csv");
        try (BufferedWriter w = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            w.write("task_id,planned_arrival_offset_micros,workload,planned_execution_micros,deadline_after_arrival_micros"); w.newLine();
            long offset = 0;
            for (int i = 0; i < s.tasks(); i++) {
                String workload = FlowGateAdapter.workloadFor(s, i);
                long interval = s.arrivalIntervalMicros();
                if (s.pattern().equals("burst") && i < s.tasks() / 5) interval = 0;
                if (s.pattern().equals("overload") && i % 5 != 0) interval = 0;
                if (s.pattern().equals("recovery") && i < s.tasks() / 2) interval = 0;
                w.write(i + "," + offset + "," + workload + "," + FlowGateAdapter.executionFor(s, i) + "," + s.deadlineMicros());
                w.newLine();
                offset += interval;
            }
        }
    }

    private static void writeEvents(Path path, List<TaskEvent> events) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            w.write(HEADER); w.newLine();
            for (TaskEvent e : events) {
                w.write(String.join(",", Long.toString(e.taskId()), e.adapter(), e.scenario(), e.workload(),
                    Long.toString(e.arrivalNanos()), Long.toString(e.startNanos()), Long.toString(e.finishNanos()),
                    e.status(), Long.toString(e.deadlineNanos()))); w.newLine();
            }
        }
    }
    private static void writeSkipped(Path output, Scenario s, BenchmarkAdapter a, String reason) throws IOException {
        Path p = output.resolve(s.id() + "_" + a.id() + "_SKIPPED.txt");
        Files.writeString(p, s.id() + " " + s.name() + "\nAdapter: " + a.displayName() + "\nStatus: NOT IMPLEMENTED\nReason: " + reason, StandardCharsets.UTF_8);
    }
    private static void writeManifest(Path output, List<Scenario> scenarios, List<BenchmarkAdapter> adapters) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(output.resolve("manifest.json"), StandardCharsets.UTF_8)) {
            w.write("{\n  \"createdAt\": \"" + Instant.now() + "\",\n  \"javaVersion\": \"" + System.getProperty("java.version") + "\",\n  \"os\": \"" + System.getProperty("os.name") + "\",\n  \"osArch\": \"" + System.getProperty("os.arch") + "\",\n  \"availableProcessors\": " + Runtime.getRuntime().availableProcessors() + ",\n  \"adapters\": [\n");
            for (int i=0;i<adapters.size();i++) { BenchmarkAdapter a=adapters.get(i); w.write("    {\"id\":\""+a.id()+"\",\"name\":\""+a.displayName()+"\"}"+(i+1<adapters.size()?",":"")+"\n"); }
            w.write("  ],\n  \"scenarios\": [\n");
            for (int i=0;i<scenarios.size();i++) { Scenario s=scenarios.get(i); w.write("    {\"id\":\""+s.id()+"\",\"name\":\""+s.name()+"\",\"tasks\":"+s.tasks()+",\"workers\":"+s.workers()+",\"queueCapacity\":"+s.queueCapacity()+",\"arrivalIntervalMicros\":"+s.arrivalIntervalMicros()+",\"executionMicros\":"+s.executionMicros()+",\"pattern\":\""+s.pattern()+"\"}"+(i+1<scenarios.size()?",":"")+"\n"); }
            w.write("  ]\n}\n");
        }
    }
}
