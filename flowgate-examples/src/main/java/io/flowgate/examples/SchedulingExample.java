package io.flowgate.examples;

import io.flowgate.api.FlowGate;
import io.flowgate.api.Priority;
import io.flowgate.api.Submission;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class SchedulingExample {

    public static void main(String[] args) throws Exception {

        FlowGate gate = FlowGate.builder()
                .globalConcurrency(4)

                .queue("high_weight", q -> q
                        .priority(Priority.HIGH)
                        .weight(5)
                        .capacity(100)
                        .maxConcurrency(3))

                .queue("low_weight", q -> q
                        .priority(Priority.NORMAL)
                        .weight(1)
                        .capacity(100)
                        .maxConcurrency(2))

                .build();

        gate.start();

        List<CompletableFuture<?>> futures = new ArrayList<>();

        for (int i = 1; i <= 10; i++) {

            int taskId = i;

            Submission<Void> submission = gate.submit(
                    "high_weight",
                    () -> executeTask("HIGH_WEIGHT", taskId)
            );

            if (submission.isAccepted()) {
                futures.add(submission.future());
            }
        }

        for (int i = 1; i <= 10; i++) {

            int taskId = i;

            Submission<Void> submission = gate.submit(
                    "low_weight",
                    () -> executeTask("LOW_WEIGHT", taskId)
            );

            if (submission.isAccepted()) {
                futures.add(submission.future());
            }
        }

        CompletableFuture.allOf(
                futures.toArray(new CompletableFuture[0])
        ).get();

        gate.close();
    }

    private static void executeTask(String workload, int id) {
        System.out.println(workload + " executing task " + id);

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}