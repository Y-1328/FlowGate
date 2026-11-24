package io.flowgate.examples;

import io.flowgate.api.FlowGate;
import io.flowgate.api.Submission;
import java.time.Duration;

public class DeadlineExample {

    public static void main(String[] args) throws Exception {

        FlowGate gate = FlowGate.builder()
                .globalConcurrency(1)
                .queue("time_sensitive", q -> q
                        .capacity(20)
                        .defaultDeadline(Duration.ofMillis(500)))
                .build();

        gate.start();

        Submission<Void> submission = gate.submit(
                "time_sensitive",
                () -> {
                    System.out.println("Executing time-sensitive task");

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
        );

        if (submission.isAccepted()) {
            System.out.println("Accepted: " + submission.id());

            try {
                submission.future().get();
            } catch (Exception e) {
                System.out.println("Task did not complete normally: " + e);
            }

            System.out.println("Final status: " + submission.status());
        }

        gate.close();
    }
}