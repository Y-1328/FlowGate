
package io.flowgate.policy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;
import java.util.function.Predicate;

/**
 * Describes whether, how often and how quickly a failed task is retried.
 *
 * <h2>Attempt semantics</h2>
 * {@code maxAttempts} counts the <em>initial execution</em>. With
 * {@code maxAttempts = 3}: attempt 1 is the initial run, attempt 2 is the first
 * retry, attempt 3 is the second retry. If attempt 3 fails, there is no further
 * retry. {@code maxAttempts = 1} means "never retry".
 *
 * <h2>What is retried</h2>
 * Only failures the policy is explicitly told to retry
 * ({@link Builder#retryOn(Class[])} / {@link Builder#retryIf(Predicate)}).
 * There is deliberately no "retry everything" default. Regardless of
 * configuration, FlowGate never retries a {@link java.lang.Error} (fatal JVM
 * conditions) or a {@link CancellationException}, and it never retries once the
 * task's deadline has elapsed.
 *
 * <h2>Backoff</h2>
 * The delay before retry <em>n</em> (after attempt <em>n</em> failed) is
 * {@code initialBackoff * multiplier^(n-1)}, capped at {@code maxBackoff}. If a
 * jitter fraction {@code j} is configured, the delay is reduced by a random
 * share of at most {@code j}: {@code delay * (1 - j * r)} with {@code r} in
 * {@code [0, 1)}. Jitter therefore never lengthens a delay beyond the cap.
 * All arithmetic is saturating; no configuration can overflow.
 *
 * <p>Instances are immutable and thread-safe, provided any custom jitter source
 * is thread-safe.
 */
public final class RetryPolicy {

    /** Upper bound for any single delay: keeps absolute due-time arithmetic overflow-safe. */
    static final long MAX_DELAY_NANOS = Long.MAX_VALUE / 4;

    private static final RetryPolicy DISABLED = new Builder().maxAttempts(1).build();

    private final int maxAttempts;
    private final List<Predicate<Throwable>> classifiers;
    private final long initialBackoffNanos;
    private final long maxBackoffNanos;
    private final double multiplier;
    private final double jitter;
    private final DoubleSupplier jitterSource;

    private RetryPolicy(Builder builder) {
        this.maxAttempts = builder.maxAttempts;
        this.classifiers = List.copyOf(builder.classifiers);
        this.initialBackoffNanos = builder.initialBackoffNanos;
        this.maxBackoffNanos = builder.maxBackoffNanos;
        this.multiplier = builder.multiplier;
        this.jitter = builder.jitter;
        this.jitterSource = builder.jitterSource;
    }

    /** A policy that never retries. This is the default for every queue. */
    public static RetryPolicy disabled() {
        return DISABLED;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Whether this policy can ever retry (i.e. {@code maxAttempts > 1}). */
    public boolean isEnabled() {
        return maxAttempts > 1;
    }

    /** Total attempts allowed, including the initial execution. */
    public int maxAttempts() {
        return maxAttempts;
    }

    /**
     * Whether this failure is eligible for retry under the classification rules.
     *
     * <p>This does not look at the attempt count or the deadline; the runtime
     * applies those separately.
     *
     * @return false if the policy is disabled, the failure is an {@link Error} or
     *         {@link CancellationException}, no classifier accepts it, or a
     *         classifier throws
     */
    public boolean shouldRetry(Throwable failure) {
        Objects.requireNonNull(failure, "failure");

        if (!isEnabled()
                || failure instanceof Error
                || failure instanceof CancellationException) {
            return false;
        }

        for (Predicate<Throwable> classifier : classifiers) {
            try {
                if (classifier.test(failure)) {
                    return true;
                }
            } catch (RuntimeException misbehavingClassifier) {
                return false;
            }
        }

        return false;
    }

    /**
     * Delay before the retry that follows the given failed attempt.
     *
     * @param failedAttempt the attempt that just failed, starting at 1
     * @return nanoseconds, at least 1 and at most the configured maximum backoff
     */
    public long backoffNanos(int failedAttempt) {
        if (failedAttempt < 1) {
            throw new IllegalArgumentException("failedAttempt must be >= 1");
        }

        // pow may overflow to Infinity; Math.min against a finite cap absorbs that.
        double scaled = initialBackoffNanos * Math.pow(multiplier, failedAttempt - 1);
        long base = (long) Math.min(scaled, (double) maxBackoffNanos);
        base = Math.max(1L, Math.min(base, maxBackoffNanos));

        if (jitter == 0.0) {
            return base;
        }

        double r = jitterSource.getAsDouble();

        if (!(r >= 0.0)) {          // also rejects NaN
            r = 0.0;
        } else if (r >= 1.0) {
            r = Math.nextDown(1.0);
        }

        long jittered = (long) (base * (1.0 - jitter * r));

        return Math.max(1L, Math.min(jittered, base));
    }

    public Duration initialBackoff() {
        return Duration.ofNanos(initialBackoffNanos);
    }

    public Duration maxBackoff() {
        return Duration.ofNanos(maxBackoffNanos);
    }

    public double multiplier() {
        return multiplier;
    }

    public double jitter() {
        return jitter;
    }

    @Override
    public String toString() {
        return isEnabled()
                ? "RetryPolicy[maxAttempts=" + maxAttempts
                + ", initialBackoff=" + initialBackoff()
                + ", maxBackoff=" + maxBackoff()
                + ", multiplier=" + multiplier
                + ", jitter=" + jitter + "]"
                : "RetryPolicy[disabled]";
    }

    /** Builds a {@link RetryPolicy}. Defaults: 3 attempts, 100 ms, x2, cap 30 s, no jitter. */
    public static final class Builder {

        private int maxAttempts = 3;
        private final List<Predicate<Throwable>> classifiers = new ArrayList<>();
        private long initialBackoffNanos = Duration.ofMillis(100).toNanos();
        private long maxBackoffNanos = Duration.ofSeconds(30).toNanos();
        private double multiplier = 2.0;
        private double jitter = 0.0;
        private DoubleSupplier jitterSource =
                () -> ThreadLocalRandom.current().nextDouble();

        private Builder() {
        }

        /** Total attempts including the initial execution; 1 disables retries. */
        public Builder maxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
            return this;
        }

        /** Retries failures that are instances of any of the given types. May be combined. */
        @SafeVarargs
        public final Builder retryOn(Class<? extends Throwable>... types) {
            Objects.requireNonNull(types, "types");

            for (Class<? extends Throwable> type : types) {
                Objects.requireNonNull(type, "type");
                classifiers.add(type::isInstance);
            }

            return this;
        }

        /** Retries failures accepted by the predicate. May be combined with other classifiers. */
        public Builder retryIf(Predicate<? super Throwable> predicate) {
            Objects.requireNonNull(predicate, "predicate");
            classifiers.add(predicate::test);
            return this;
        }

        public Builder initialBackoff(Duration initialBackoff) {
            this.initialBackoffNanos = saturatedNanos(initialBackoff, "initialBackoff");
            return this;
        }

        public Builder maxBackoff(Duration maxBackoff) {
            this.maxBackoffNanos = saturatedNanos(maxBackoff, "maxBackoff");
            return this;
        }

        /** Growth factor per retry; must be finite and at least 1. */
        public Builder multiplier(double multiplier) {
            this.multiplier = multiplier;
            return this;
        }

        /** Maximum share of a delay that jitter may remove, in [0, 1]. Default 0 (none). */
        public Builder jitter(double fraction) {
            this.jitter = fraction;
            return this;
        }

        /**
         * Supplies values in [0, 1) for jitter. Intended for deterministic tests;
         * the default uses {@link ThreadLocalRandom}.
         */
        public Builder jitterSource(DoubleSupplier source) {
            this.jitterSource = Objects.requireNonNull(source, "source");
            return this;
        }

        /**
         * @throws IllegalStateException if retries are enabled but no failure
         *         classification was configured
         * @throws IllegalArgumentException if any value is out of range
         */
        public RetryPolicy build() {
            if (maxAttempts < 1) {
                throw new IllegalArgumentException("maxAttempts must be >= 1");
            }

            if (initialBackoffNanos <= 0) {
                throw new IllegalArgumentException("initialBackoff must be positive");
            }

            if (maxBackoffNanos < initialBackoffNanos) {
                throw new IllegalArgumentException(
                        "maxBackoff must be >= initialBackoff");
            }

            if (Double.isNaN(multiplier)
                    || Double.isInfinite(multiplier)
                    || multiplier < 1.0) {
                throw new IllegalArgumentException(
                        "multiplier must be finite and >= 1");
            }

            if (Double.isNaN(jitter) || jitter < 0.0 || jitter > 1.0) {
                throw new IllegalArgumentException("jitter must be within [0, 1]");
            }

            if (maxAttempts > 1 && classifiers.isEmpty()) {
                throw new IllegalStateException(
                        "Retries are enabled (maxAttempts > 1) but no failure "
                                + "classification was configured; call retryOn(...) "
                                + "or retryIf(...). FlowGate never retries arbitrary "
                                + "failures implicitly.");
            }

            return new RetryPolicy(this);
        }

        private static long saturatedNanos(Duration duration, String name) {
            Objects.requireNonNull(duration, name);

            if (duration.isZero() || duration.isNegative()) {
                throw new IllegalArgumentException(name + " must be positive");
            }

            try {
                return Math.min(duration.toNanos(), MAX_DELAY_NANOS);
            } catch (ArithmeticException tooLarge) {
                return MAX_DELAY_NANOS;
            }
        }
    }
}
