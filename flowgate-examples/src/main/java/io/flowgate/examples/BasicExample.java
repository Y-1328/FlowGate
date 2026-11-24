package io.flowgate.examples;

import io.flowgate.api.FlowGate;
import io.flowgate.api.Priority;
import io.flowgate.api.Submission;

public class BasicExample {

    public static void main(String[] args) throws Exception {

        FlowGate gate = FlowGate.builder()
                .globalConcurrency(2)
                .queue("default", q -> q
                        .priority(Priority.NORMAL)
                        .capacity(10))
                .build();

        gate.start();

        Submission<Void> submission = gate.submit(
                "default",
                () -> System.out.println("Task is executing!")
        );

        if (submission.isAccepted()) {
            System.out.println("Task accepted: " + submission.id());

            submission.future().get();

            System.out.println("Task completed.");
        } else {
            System.out.println("Task rejected: "
                    + submission.rejectionReason());
        }

        gate.close();
    }
}