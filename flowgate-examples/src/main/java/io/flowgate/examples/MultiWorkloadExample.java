package io.flowgate.examples;

import io.flowgate.api.FlowGate;
import io.flowgate.api.Priority;
import io.flowgate.api.Submission;

import java.util.concurrent.CompletableFuture;

public class MultiWorkloadExample {

    public static void main(String[] args) throws Exception {

        FlowGate gate = FlowGate.builder()
                .globalConcurrency(8)

                .queue("payments", q -> q
                        .priority(Priority.CRITICAL)
                        .weight(5)
                        .capacity(100)
                        .reservedConcurrency(2)
                        .maxConcurrency(4))

                .queue("notifications", q -> q
                        .priority(Priority.NORMAL)
                        .weight(1)
                        .capacity(500))

                .queue("analytics", q -> q
                        .priority(Priority.LOW)
                        .weight(2)
                        .capacity(200)
                        .maxConcurrency(2))

                .build();

        gate.start();

        Submission<Void> payment = gate.submit(
                "payments",
                () -> processPayment()
        );

        Submission<Void> notification = gate.submit(
                "notifications",
                () -> sendNotification()
        );

        Submission<Void> report = gate.submit(
                "analytics",
                () -> generateReport()
        );

        CompletableFuture.allOf(
                payment.future(),
                notification.future(),
                report.future()
        ).get();

        gate.close();
    }

    private static void processPayment() {
        System.out.println("[PAYMENTS] Processing payment");
    }

    private static void sendNotification() {
        System.out.println("[NOTIFICATIONS] Sending notification");
    }

    private static void generateReport() {
        System.out.println("[ANALYTICS] Generating report");
    }
}