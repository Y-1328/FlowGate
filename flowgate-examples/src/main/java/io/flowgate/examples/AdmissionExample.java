package io.flowgate.examples;

import io.flowgate.api.FlowGate;
import io.flowgate.api.Submission;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AdmissionExample {

    public static void main(String[] args) throws Exception {

        CountDownLatch workerOccupied = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);

        AtomicInteger executionCount = new AtomicInteger(0);

        try (FlowGate gate = FlowGate.builder()
                .globalConcurrency(1)
                .queue("limited", q -> q
                        .capacity(1)
                        .maxConcurrency(1))
                .build()) {

            gate.start();

            // TASK 1: Occupy the only worker.
            Submission<Void> task1 = gate.submit("limited", () -> {
                executionCount.incrementAndGet();

                System.out.println("Task 1 STARTED - worker occupied");

                workerOccupied.countDown();

                try {
                    releaseWorker.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Task 1 interrupted", e);
                }

                System.out.println("Task 1 FINISHED");
            });

            if (!task1.isAccepted()) {
                throw new IllegalStateException("Task 1 unexpectedly rejected");
            }

            // Ensure Task 1 is actually occupying the worker.
            if (!workerOccupied.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Task 1 failed to start");
            }

            System.out.println("Worker: 1/1 occupied");

            // TASK 2: Fill the single queue slot.
            Submission<Void> task2 = gate.submit("limited", () -> {
                executionCount.incrementAndGet();
                System.out.println("Task 2 EXECUTED");
            });

            if (!task2.isAccepted()) {
                throw new IllegalStateException("Task 2 unexpectedly rejected");
            }

            System.out.println("Queue: 1/1 occupied");

            // TASK 3: Attempt admission with all capacity exhausted.
            Submission<Void> task3 = gate.submit("limited", () -> {
                executionCount.incrementAndGet();
                System.out.println("ERROR: Task 3 should never execute!");
            });

            if (task3.isAccepted()) {
                throw new AssertionError(
                        "Task 3 was accepted despite exhausted capacity");
            }

            System.out.println("Task 3 REJECTED as expected");
            System.out.println("Rejection reason: " + task3.rejectionReason());

            // Release Task 1 so the accepted queue can drain.
            releaseWorker.countDown();

            // Wait for all accepted tasks.
            task1.future().get(5, TimeUnit.SECONDS);
            task2.future().get(5, TimeUnit.SECONDS);

            // Verify behavior.
            if (executionCount.get() != 2) {
                throw new AssertionError(
                        "Expected 2 executions, got " + executionCount.get());
            }

            System.out.println("\n========== RESULTS ==========");
            System.out.println("Accepted tasks : 2");
            System.out.println("Rejected tasks : 1");
            System.out.println("Executed tasks : " + executionCount.get());
            System.out.println("Admission boundary verified.");
        } finally {
            // Always unblock the worker, even if an assertion fails.
            releaseWorker.countDown();
        }
    }
}