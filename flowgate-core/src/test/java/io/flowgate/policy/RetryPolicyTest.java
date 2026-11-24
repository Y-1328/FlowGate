
package io.flowgate.policy;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RetryPolicyTest {

    private static final long MS = 1_000_000L;

    private static RetryPolicy.Builder valid() {
        return RetryPolicy.builder().retryOn(IOException.class);
    }

    // ---- enablement and attempt semantics --------------------------------

    @Test
    void disabledPolicyNeverRetries() {
        RetryPolicy disabled = RetryPolicy.disabled();

        assertFalse(disabled.isEnabled());
        assertEquals(1, disabled.maxAttempts());
        assertFalse(disabled.shouldRetry(new IOException()));
        assertSame(disabled, RetryPolicy.disabled());
    }

    @Test
    void maxAttemptsCountsTheInitialExecution() {
        RetryPolicy three = valid().maxAttempts(3).build();

        assertTrue(three.isEnabled());
        assertEquals(3, three.maxAttempts());
    }

    @Test
    void oneAttemptMeansRetriesAreDisabledEvenWithAClassifier() {
        RetryPolicy one = valid().maxAttempts(1).build();

        assertFalse(one.isEnabled());
        assertFalse(one.shouldRetry(new IOException()));
    }

    @Test
    void retriesRequireExplicitClassification() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> RetryPolicy.builder().maxAttempts(3).build());

        assertTrue(e.getMessage().contains("retryOn"));
    }

    @Test
    void classificationIsNotRequiredWhenRetriesAreDisabled() {
        assertDoesNotThrow(() -> RetryPolicy.builder().maxAttempts(1).build());
    }

    // ---- validation -------------------------------------------------------

    @Test
    void rejectsInvalidValues() {
        assertThrows(IllegalArgumentException.class, () -> valid().maxAttempts(0).build());
        assertThrows(IllegalArgumentException.class, () -> valid().maxAttempts(-1).build());
        assertThrows(IllegalArgumentException.class, () -> valid().initialBackoff(Duration.ZERO));
        assertThrows(IllegalArgumentException.class,
                () -> valid().initialBackoff(Duration.ofMillis(-1)));
        assertThrows(IllegalArgumentException.class, () -> valid().maxBackoff(Duration.ZERO));
        assertThrows(NullPointerException.class, () -> valid().initialBackoff(null));
        assertThrows(IllegalArgumentException.class, () -> valid().multiplier(0.99).build());
        assertThrows(IllegalArgumentException.class,
                () -> valid().multiplier(Double.NaN).build());
        assertThrows(IllegalArgumentException.class,
                () -> valid().multiplier(Double.POSITIVE_INFINITY).build());
        assertThrows(IllegalArgumentException.class, () -> valid().jitter(-0.1).build());
        assertThrows(IllegalArgumentException.class, () -> valid().jitter(1.1).build());
        assertThrows(IllegalArgumentException.class, () -> valid().jitter(Double.NaN).build());
        assertThrows(NullPointerException.class, () -> valid().jitterSource(null));
        assertThrows(NullPointerException.class, () -> valid().retryIf(null));
        assertThrows(NullPointerException.class,
                () -> valid().retryOn((Class<? extends Throwable>) null));
    }

    @Test
    void maxBackoffMustNotBeBelowInitialBackoff() {
        assertThrows(IllegalArgumentException.class, () -> valid()
                .initialBackoff(Duration.ofSeconds(2))
                .maxBackoff(Duration.ofSeconds(1))
                .build());
    }

    // ---- failure classification ------------------------------------------

    @Test
    void retriesConfiguredTypesAndTheirSubclasses() {
        RetryPolicy policy = valid().build();

        assertTrue(policy.shouldRetry(new IOException()));
        assertTrue(policy.shouldRetry(new java.io.FileNotFoundException()));
        assertFalse(policy.shouldRetry(new IllegalStateException()));
    }

    @Test
    void classifiersCombine() {
        RetryPolicy policy = RetryPolicy.builder()
                .retryOn(IOException.class, TimeoutException.class)
                .retryIf(t -> t.getMessage() != null && t.getMessage().startsWith("transient"))
                .build();

        assertTrue(policy.shouldRetry(new IOException()));
        assertTrue(policy.shouldRetry(new TimeoutException()));
        assertTrue(policy.shouldRetry(new IllegalStateException("transient: busy")));
        assertFalse(policy.shouldRetry(new IllegalStateException("permanent")));
    }

    @Test
    void predicateSeesTheFailureAsThrown() {
        RetryPolicy policy = RetryPolicy.builder()
                .retryIf(t -> t instanceof UncheckedIOException u
                        && u.getCause() instanceof IOException)
                .build();

        assertTrue(policy.shouldRetry(new UncheckedIOException(new IOException())));
        assertFalse(policy.shouldRetry(new IOException()));
    }

    @Test
    void fatalErrorsAreNeverRetriedEvenWhenTheClassifierAcceptsThem() {
        RetryPolicy everything = RetryPolicy.builder().retryOn(Throwable.class).build();

        assertFalse(everything.shouldRetry(new OutOfMemoryError()));
        assertFalse(everything.shouldRetry(new StackOverflowError()));
        assertFalse(everything.shouldRetry(new AssertionError()));
        assertTrue(everything.shouldRetry(new Exception()));
    }

    @Test
    void cancellationIsNeverRetriedEvenWhenTheClassifierAcceptsIt() {
        RetryPolicy everything = RetryPolicy.builder().retryOn(Throwable.class).build();

        assertFalse(everything.shouldRetry(new CancellationException()));
    }

    @Test
    void aClassifierThatThrowsMeansDoNotRetry() {
        AtomicInteger calls = new AtomicInteger();

        RetryPolicy policy = RetryPolicy.builder()
                .retryIf(t -> {
                    calls.incrementAndGet();
                    throw new IllegalStateException("buggy classifier");
                })
                .build();

        assertFalse(policy.shouldRetry(new IOException()));
        assertEquals(1, calls.get());
    }

    // ---- backoff ----------------------------------------------------------

    @Test
    void backoffStartsAtTheInitialDelayAndGrowsExponentially() {
        RetryPolicy policy = valid()
                .initialBackoff(Duration.ofMillis(100))
                .maxBackoff(Duration.ofSeconds(60))
                .multiplier(2.0)
                .build();

        assertEquals(100 * MS, policy.backoffNanos(1));
        assertEquals(200 * MS, policy.backoffNanos(2));
        assertEquals(400 * MS, policy.backoffNanos(3));
        assertEquals(800 * MS, policy.backoffNanos(4));
    }

    @Test
    void backoffIsCappedAtTheMaximum() {
        RetryPolicy policy = valid()
                .initialBackoff(Duration.ofMillis(100))
                .maxBackoff(Duration.ofMillis(350))
                .multiplier(2.0)
                .build();

        assertEquals(100 * MS, policy.backoffNanos(1));
        assertEquals(200 * MS, policy.backoffNanos(2));
        assertEquals(350 * MS, policy.backoffNanos(3));
        assertEquals(350 * MS, policy.backoffNanos(50));
    }

    @Test
    void multiplierOneGivesAConstantDelay() {
        RetryPolicy policy = valid().initialBackoff(Duration.ofMillis(50))
                .multiplier(1.0).build();

        assertEquals(50 * MS, policy.backoffNanos(1));
        assertEquals(50 * MS, policy.backoffNanos(1_000));
    }

    @Test
    void backoffNeverOverflows() {
        RetryPolicy extreme = valid()
                .initialBackoff(Duration.ofSeconds(Long.MAX_VALUE))
                .maxBackoff(Duration.ofSeconds(Long.MAX_VALUE))
                .multiplier(Double.MAX_VALUE)
                .build();

        long cap = Long.MAX_VALUE / 4;

        for (int attempt : new int[]{1, 2, 3, 64, 1_000, Integer.MAX_VALUE}) {
            long delay = extreme.backoffNanos(attempt);
            assertTrue(delay > 0 && delay <= cap, "attempt " + attempt + " -> " + delay);
        }

        RetryPolicy hugeMultiplier = valid()
                .initialBackoff(Duration.ofMillis(1))
                .maxBackoff(Duration.ofHours(1))
                .multiplier(1e300)
                .build();

        assertEquals(Duration.ofMillis(1).toNanos(), hugeMultiplier.backoffNanos(1));
        assertEquals(Duration.ofHours(1).toNanos(), hugeMultiplier.backoffNanos(2));
        assertEquals(Duration.ofHours(1).toNanos(), hugeMultiplier.backoffNanos(500));
    }

    @Test
    void backoffRejectsNonPositiveAttemptNumbers() {
        assertThrows(IllegalArgumentException.class, () -> valid().build().backoffNanos(0));
    }

    // ---- jitter -----------------------------------------------------------

    @Test
    void zeroJitterNeverConsultsTheRandomSource() {
        AtomicInteger calls = new AtomicInteger();

        RetryPolicy policy = valid().initialBackoff(Duration.ofMillis(100))
                .jitterSource(() -> {
                    calls.incrementAndGet();
                    return 0.5;
                }).build();

        assertEquals(100 * MS, policy.backoffNanos(1));
        assertEquals(0, calls.get());
    }

    @Test
    void jitterRemovesAtMostTheConfiguredShare() {
        double[] r = {0.0};

        RetryPolicy policy = valid()
                .initialBackoff(Duration.ofMillis(1_000))
                .jitter(0.25)
                .jitterSource(() -> r[0])
                .build();

        r[0] = 0.0;
        assertEquals(1_000 * MS, policy.backoffNanos(1));

        r[0] = 0.5;
        assertEquals(875 * MS, policy.backoffNanos(1));

        r[0] = Math.nextDown(1.0);
        long lowest = policy.backoffNanos(1);
        assertTrue(lowest >= 750 * MS && lowest < 751 * MS);
    }

    @Test
    void jitteredDelayNeverExceedsTheCapOrFallsBelowOneNanosecond() {
        RetryPolicy policy = valid()
                .initialBackoff(Duration.ofMillis(10))
                .maxBackoff(Duration.ofMillis(40))
                .jitter(1.0)
                .jitterSource(() -> 0.9999999)
                .build();

        for (int attempt = 1; attempt <= 20; attempt++) {
            long delay = policy.backoffNanos(attempt);
            assertTrue(delay >= 1 && delay <= 40 * MS);
        }
    }

    @Test
    void outOfRangeRandomValuesAreClamped() {
        double[] r = {0.0};

        RetryPolicy policy = valid()
                .initialBackoff(Duration.ofMillis(100))
                .jitter(0.5)
                .jitterSource(() -> r[0])
                .build();

        for (double bad : new double[]{-5.0, Double.NaN, 7.0, Double.POSITIVE_INFINITY}) {
            r[0] = bad;
            long delay = policy.backoffNanos(1);
            assertTrue(delay >= 50 * MS && delay <= 100 * MS, "r=" + bad + " -> " + delay);
        }
    }

    @Test
    void defaultJitterSourceStaysWithinBounds() {
        RetryPolicy policy = valid().initialBackoff(Duration.ofMillis(100))
                .jitter(0.5).build();

        for (int i = 0; i < 1_000; i++) {
            long delay = policy.backoffNanos(1);
            assertTrue(delay >= 50 * MS && delay <= 100 * MS);
        }
    }

    @Test
    void sameJitterSequenceProducesTheSameDelays() {
        double[] sequence = {0.1, 0.9, 0.5};

        long[] first = delaysFor(sequence);
        long[] second = delaysFor(sequence);

        assertArrayEquals(first, second);
    }

    private static long[] delaysFor(double[] sequence) {
        int[] index = {0};

        RetryPolicy policy = valid().initialBackoff(Duration.ofMillis(100)).jitter(0.4)
                .jitterSource(() -> sequence[index[0]++ % sequence.length]).build();

        return new long[]{policy.backoffNanos(1), policy.backoffNanos(2), policy.backoffNanos(3)};
    }

    @Test
    void toStringDescribesTheConfiguration() {
        assertEquals("RetryPolicy[disabled]", RetryPolicy.disabled().toString());
        assertTrue(valid().maxAttempts(4).build().toString().contains("maxAttempts=4"));
    }
}
