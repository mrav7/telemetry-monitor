package io.github.mrav7.telemetrymonitor.lifecycle;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/** A single monotonic time budget shared by every phase of coordinated shutdown. */
public final class ShutdownDeadline {

    private final long startedAtNanos;
    private final long graceNanos;
    private final LongSupplier nanoTime;

    /** Starts a deadline using the JVM's monotonic clock. */
    public static ShutdownDeadline start(Duration grace) {
        return new ShutdownDeadline(grace, System::nanoTime);
    }

    ShutdownDeadline(Duration grace, LongSupplier nanoTime) {
        Objects.requireNonNull(grace, "grace");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        if (grace.isNegative() || grace.isZero()) {
            throw new IllegalArgumentException("shutdown grace must be positive");
        }
        startedAtNanos = nanoTime.getAsLong();
        graceNanos = grace.toNanos();
    }

    /** Remaining time, clamped to zero after expiry. */
    public Duration remaining() {
        return Duration.ofNanos(remainingNanos());
    }

    /** Remaining nanoseconds, clamped to zero after expiry. */
    public long remainingNanos() {
        return Math.max(0L, graceNanos - elapsedNanos());
    }

    public boolean expired() {
        return remainingNanos() == 0L;
    }

    public Duration elapsed() {
        return Duration.ofNanos(elapsedNanos());
    }

    /** Waits for an executor using only the budget still available now. */
    public boolean awaitTermination(java.util.concurrent.ExecutorService executor)
            throws InterruptedException {
        return executor.awaitTermination(remainingNanos(), TimeUnit.NANOSECONDS);
    }

    private long elapsedNanos() {
        return Math.max(0L, nanoTime.getAsLong() - startedAtNanos);
    }
}
