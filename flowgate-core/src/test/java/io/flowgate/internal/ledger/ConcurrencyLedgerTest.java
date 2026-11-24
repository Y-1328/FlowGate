
package io.flowgate.internal.ledger;

import io.flowgate.api.Priority;
import io.flowgate.config.FlowGateConfig;
import io.flowgate.config.QueueConfig;
import io.flowgate.internal.queue.GlobalAdmissionGate;
import io.flowgate.internal.queue.QueueRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ConcurrencyLedgerTest {

    private QueueConfig queue(
            String id,
            Priority priority,
            int reserved,
            int maximum
    ) {
        return new QueueConfig(
                id, priority, 1, 100, reserved, maximum, null
        );
    }

    private QueueRegistry registry(FlowGateConfig config) {
        // Replace this constructor call if your GlobalAdmissionGate
        // uses a different constructor signature.
        GlobalAdmissionGate gate = new GlobalAdmissionGate(100);
        return new QueueRegistry(config.queues(), gate);
    }

    @Test
    void grantsReservedCapacity() {
        ConcurrencyLedger ledger =
                new ConcurrencyLedger(4, new int[]{2, 1}, new int[]{4, 4});

        ExecutionPermit first = ledger.tryAcquire(0, new int[]{0, 0});
        ExecutionPermit second = ledger.tryAcquire(0, new int[]{0, 0});

        assertNotNull(first);
        assertNotNull(second);
        assertFalse(first.wasBorrowed());
        assertFalse(second.wasBorrowed());
        assertEquals(2, ledger.active(0));

        first.close();
        second.close();
    }

    @Test
    void grantsBorrowedCapacityWhenNoOtherQueueNeedsReservation() {
        ConcurrencyLedger ledger =
                new ConcurrencyLedger(4, new int[]{1, 1}, new int[]{4, 4});

        ExecutionPermit reserved = ledger.tryAcquire(0, new int[]{0, 0});
        ExecutionPermit borrowed = ledger.tryAcquire(0, new int[]{0, 0});

        assertNotNull(reserved);
        assertNotNull(borrowed);
        assertFalse(reserved.wasBorrowed());
        assertTrue(borrowed.wasBorrowed());
        assertEquals(1, ledger.borrowed(0));

        reserved.close();
        borrowed.close();
    }

    @Test
    void protectsOtherQueuesUnmetReservationDemand() {
        ConcurrencyLedger ledger =
                new ConcurrencyLedger(4, new int[]{1, 2}, new int[]{4, 4});

        ExecutionPermit reserved = ledger.tryAcquire(0, new int[]{0, 2});
        assertNotNull(reserved);

        ExecutionPermit firstBorrow = ledger.tryAcquire(0, new int[]{0, 2});

        assertNotNull(firstBorrow);
        assertTrue(firstBorrow.wasBorrowed());

        assertNull(ledger.tryAcquire(0, new int[]{0, 2}));

        reserved.close();
        firstBorrow.close();
    }

    @Test
    void reservedGrantIsNotBlockedByOtherQueuesDemand() {
        ConcurrencyLedger ledger =
                new ConcurrencyLedger(4, new int[]{1, 2}, new int[]{4, 4});

        ExecutionPermit permit = ledger.tryAcquire(1, new int[]{0, 2});

        assertNotNull(permit);
        assertFalse(permit.wasBorrowed());
        assertEquals(1, ledger.active(1));

        permit.close();
    }

    @Test
    void deniesWhenGlobalConcurrencyIsExhausted() {
        ConcurrencyLedger ledger =
                new ConcurrencyLedger(2, new int[]{1, 1}, new int[]{2, 2});

        ExecutionPermit first = ledger.tryAcquire(0, new int[]{0, 0});
        ExecutionPermit second = ledger.tryAcquire(1, new int[]{0, 0});

        assertNotNull(first);
        assertNotNull(second);
        assertNull(ledger.tryAcquire(0, new int[]{0, 0}));

        assertEquals(
                AllocationDecision.DENY_GLOBAL_LIMIT,
                ledger.check(0, new int[]{0, 0})
        );

        first.close();
        second.close();
    }

    @Test
    void enforcesPerQueueMaximumConcurrency() {
        ConcurrencyLedger ledger =
                new ConcurrencyLedger(4, new int[]{1, 1}, new int[]{2, 4});

        ExecutionPermit first = ledger.tryAcquire(0, new int[]{0, 0});
        ExecutionPermit second = ledger.tryAcquire(0, new int[]{0, 0});

        assertNotNull(first);
        assertNotNull(second);
        assertNull(ledger.tryAcquire(0, new int[]{0, 0}));

        assertEquals(
                AllocationDecision.DENY_QUEUE_MAX,
                ledger.check(0, new int[]{0, 0})
        );

        first.close();
        second.close();
    }

    @Test
    void releaseIsIdempotent() {
        ConcurrencyLedger ledger =
                new ConcurrencyLedger(2, new int[]{1}, new int[]{2});

        ExecutionPermit permit = ledger.tryAcquire(0, new int[]{0});

        assertNotNull(permit);
        assertEquals(1, ledger.activeTotal());

        permit.close();
        permit.close();
        permit.close();

        assertTrue(permit.isReleased());
        assertEquals(0, ledger.activeTotal());
        assertEquals(2, ledger.available());
        assertEquals(0, ledger.active(0));
    }

    @Test
    void borrowedCountIsDerivedFromCurrentActiveCount() {
        ConcurrencyLedger ledger =
                new ConcurrencyLedger(4, new int[]{1, 1}, new int[]{4, 4});

        ExecutionPermit reserved = ledger.tryAcquire(0, new int[]{0, 0});
        ExecutionPermit borrowed = ledger.tryAcquire(0, new int[]{0, 0});

        assertEquals(1, ledger.borrowed(0));

        reserved.close();

        assertEquals(0, ledger.borrowed(0));

        borrowed.close();

        assertEquals(0, ledger.borrowed(0));
    }

    @Test
    void reportsAvailableCapacityCorrectly() {
        ConcurrencyLedger ledger =
                new ConcurrencyLedger(5, new int[]{2}, new int[]{5});

        assertEquals(5, ledger.available());
        assertEquals(0, ledger.activeTotal());

        ExecutionPermit first = ledger.tryAcquire(0, new int[]{0});

        assertEquals(4, ledger.available());
        assertEquals(1, ledger.activeTotal());

        first.close();

        assertEquals(5, ledger.available());
        assertEquals(0, ledger.activeTotal());
    }

    @Test
    void snapshotEligibilityReflectsCurrentAllocationRules() {
        ConcurrencyLedger ledger =
                new ConcurrencyLedger(3, new int[]{1, 1}, new int[]{3, 3});

        boolean[] eligible = new boolean[2];

        ledger.snapshotEligibility(eligible, new int[]{0, 1});

        assertTrue(eligible[0]);
        assertTrue(eligible[1]);

        ExecutionPermit first = ledger.tryAcquire(0, new int[]{0, 1});
        assertNotNull(first);

        ExecutionPermit borrowed = ledger.tryAcquire(0, new int[]{0, 1});
        assertNotNull(borrowed);

        ledger.snapshotEligibility(eligible, new int[]{0, 1});

        assertFalse(eligible[0]);
        assertTrue(eligible[1]);

        first.close();
        borrowed.close();
    }

    @Test
    void rejectsInvalidQueueIndex() {
        ConcurrencyLedger ledger =
                new ConcurrencyLedger(2, new int[]{1}, new int[]{2});

        assertThrows(
                IllegalArgumentException.class,
                () -> ledger.tryAcquire(1, new int[]{0})
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> ledger.active(-1)
        );
    }

    @Test
    void rejectsInvalidWaitingSnapshots() {
        ConcurrencyLedger ledger =
                new ConcurrencyLedger(3, new int[]{1, 1}, new int[]{3, 3});

        assertThrows(
                IllegalArgumentException.class,
                () -> ledger.tryAcquire(0, new int[]{0})
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> ledger.tryAcquire(0, new int[]{0, -1})
        );

        assertThrows(
                NullPointerException.class,
                () -> ledger.tryAcquire(0, null)
        );
    }

    @Test
    void rejectsInvalidConfiguration() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new ConcurrencyLedger(0, new int[]{1}, new int[]{1})
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> new ConcurrencyLedger(3, new int[]{1}, new int[]{1, 2})
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> new ConcurrencyLedger(3, new int[]{-1}, new int[]{2})
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> new ConcurrencyLedger(3, new int[]{2}, new int[]{1})
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> new ConcurrencyLedger(3, new int[]{1}, new int[]{4})
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> new ConcurrencyLedger(3, new int[]{2, 2}, new int[]{3, 3})
        );
    }

    @Test
    void exposesConfiguredQueueCountAndGlobalBudget() {
        ConcurrencyLedger ledger =
                new ConcurrencyLedger(8, new int[]{2, 2, 1}, new int[]{4, 4, 3});

        assertEquals(3, ledger.queueCount());
        assertEquals(8, ledger.globalConcurrency());
    }

    @Test
    void createsLedgerFromRegistry() {
        FlowGateConfig config = new FlowGateConfig(
                8,
                List.of(
                        queue("payments", Priority.HIGH, 2, 4),
                        queue("notifications", Priority.NORMAL, 1, 3)
                )
        );

        QueueRegistry registry = registry(config);
        ConcurrencyLedger ledger =
                new ConcurrencyLedger(config.globalConcurrency(), registry);

        assertEquals(8, ledger.globalConcurrency());
        assertEquals(2, ledger.queueCount());

        ExecutionPermit permit = ledger.tryAcquire(0, new int[]{0, 0});

        assertNotNull(permit);
        assertFalse(permit.wasBorrowed());

        permit.close();
    }

    @Test
    void registryQueueOrderMatchesLedgerIndices() {
        FlowGateConfig config = new FlowGateConfig(
                8,
                List.of(
                        queue("payments", Priority.HIGH, 2, 4),
                        queue("notifications", Priority.NORMAL, 1, 3)
                )
        );

        QueueRegistry registry = registry(config);
        ConcurrencyLedger ledger =
                new ConcurrencyLedger(config.globalConcurrency(), registry);

        assertEquals("payments", registry.ordered()[0].id());
        assertEquals("notifications", registry.ordered()[1].id());

        ExecutionPermit permit = ledger.tryAcquire(1, new int[]{0, 0});

        assertNotNull(permit);
        assertFalse(permit.wasBorrowed());
        assertEquals(1, ledger.active(1));

        permit.close();
    }

    @Test
    void rejectsNullRegistry() {
        assertThrows(
                NullPointerException.class,
                () -> new ConcurrencyLedger(4, (QueueRegistry) null)
        );
    }

    @Test
    void doesNotRetainCallerWaitingSnapshot() {
        ConcurrencyLedger ledger =
                new ConcurrencyLedger(4, new int[]{1, 1}, new int[]{4, 4});

        int[] waiting = {0, 2};

        ExecutionPermit reserved =
                ledger.tryAcquire(0, waiting);

        assertNotNull(reserved);

        // Mutating the caller's array must not alter an already
        // completed allocation decision or ledger accounting.
        waiting[1] = 0;

        assertEquals(1, ledger.active(0));
        assertEquals(1, ledger.activeTotal());

        reserved.close();
    }

    @Test
    void doesNotRetainConstructorConfigurationArrays() {
        int[] reserved = {1, 1};
        int[] maximum = {3, 3};

        ConcurrencyLedger ledger =
                new ConcurrencyLedger(4, reserved, maximum);

        reserved[0] = 3;
        maximum[0] = 1;

        ExecutionPermit first =
                ledger.tryAcquire(0, new int[]{0, 0});

        ExecutionPermit second =
                ledger.tryAcquire(0, new int[]{0, 0});

        assertNotNull(first);
        assertNotNull(second);

        assertEquals(2, ledger.active(0));

        first.close();
        second.close();
    }

    @Test
    void snapshotEligibilityOverwritesEveryDestinationEntry() {
        ConcurrencyLedger ledger =
                new ConcurrencyLedger(2, new int[]{1, 1}, new int[]{2, 2});

        boolean[] eligible = {false, false};

        ledger.snapshotEligibility(eligible, new int[]{0, 0});

        assertTrue(eligible[0]);
        assertTrue(eligible[1]);

        ExecutionPermit first =
                ledger.tryAcquire(0, new int[]{0, 0});

        ExecutionPermit second =
                ledger.tryAcquire(1, new int[]{0, 0});

        assertNotNull(first);
        assertNotNull(second);

        ledger.snapshotEligibility(eligible, new int[]{0, 0});

        assertFalse(eligible[0]);
        assertFalse(eligible[1]);

        first.close();
        second.close();
    }

}
