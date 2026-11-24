package io.flowgate.internal.BT02_Executable;

import io.flowgate.internal.ledger.ConcurrencyLedger;

import java.util.concurrent.*;

final class BT02TestSupport {
    private BT02TestSupport() {}

    static void await(CountDownLatch latch) {
        try {
            if (!latch.await(20, TimeUnit.SECONDS)) throw new AssertionError("Barrier timed out");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted", e);
        }
    }

    static void shutdown(ExecutorService pool) {
        pool.shutdown();
        try {
            if (!pool.awaitTermination(20, TimeUnit.SECONDS)) {
                pool.shutdownNow();
                if (!pool.awaitTermination(10, TimeUnit.SECONDS)) throw new AssertionError("Executor did not terminate");
            }
        } catch (InterruptedException e) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted during shutdown", e);
        }
    }


    static void assertLedgerConsistent(ConcurrencyLedger ledger, int queues, int globalLimit) {
        int sum = 0;
        for (int q = 0; q < queues; q++) {
            int active = ledger.active(q);
            if (active < 0) throw new AssertionError("Negative active count for queue " + q);
            sum += active;
        }
        if (sum != ledger.activeTotal()) throw new AssertionError("Queue sum " + sum + " != ledger total " + ledger.activeTotal());
        if (ledger.activeTotal() < 0 || ledger.activeTotal() > globalLimit) throw new AssertionError("Global limit/accounting violation: " + ledger.activeTotal());
        if (ledger.available() != globalLimit - ledger.activeTotal()) throw new AssertionError("Available capacity inconsistent");
    }
}
