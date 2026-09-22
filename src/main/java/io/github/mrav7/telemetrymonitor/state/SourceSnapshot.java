package io.github.mrav7.telemetrymonitor.state;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * The observable state of one source at a point in time.
 *
 * <p>Instances are immutable, and every state change produces a new one. That is what makes an
 * update atomic from a reader's point of view: a snapshot handed out by the {@link SourceRegistry}
 * is always internally consistent, never a half-applied mixture of two events.
 *
 * <p>Both timestamps come from the monitor's own clock, by way of
 * {@link io.github.mrav7.telemetrymonitor.protocol.TelemetryEnvelope#receivedAt()}. The time
 * declared by the remote process is data, never a liveness signal, so it plays no part here.
 *
 * @param sourceId            identifier declared by the source
 * @param status              current operational state
 * @param firstAcceptedAt     reception time of the event that admitted this source; never changes
 * @param lastAcceptedAt      reception time of the most recent accepted event; never moves backward
 * @param acceptedEventCount  events accepted for this source; never decreases
 * @param processedEventCount events whose processing completed; never decreases, never exceeds
 *     {@code acceptedEventCount}
 */
public record SourceSnapshot(
        String sourceId,
        SourceStatus status,
        Instant firstAcceptedAt,
        Instant lastAcceptedAt,
        long acceptedEventCount,
        long processedEventCount) {

    public SourceSnapshot {
        Objects.requireNonNull(sourceId, "sourceId");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(firstAcceptedAt, "firstAcceptedAt");
        Objects.requireNonNull(lastAcceptedAt, "lastAcceptedAt");
        if (lastAcceptedAt.isBefore(firstAcceptedAt)) {
            throw new IllegalArgumentException(
                    "lastAcceptedAt must not precede firstAcceptedAt for source " + sourceId);
        }
        if (acceptedEventCount < 0 || processedEventCount < 0) {
            throw new IllegalArgumentException("event counts must not be negative for " + sourceId);
        }
        if (processedEventCount > acceptedEventCount) {
            throw new IllegalArgumentException(
                    "processedEventCount must not exceed acceptedEventCount for " + sourceId);
        }
    }

    /** The state a source is admitted with: online, counted once, both timestamps at reception. */
    static SourceSnapshot firstAccepted(String sourceId, Instant receivedAt) {
        Objects.requireNonNull(receivedAt, "receivedAt");
        return new SourceSnapshot(sourceId, SourceStatus.ONLINE, receivedAt, receivedAt, 1, 1);
    }

    /**
     * The state after one further accepted event.
     *
     * <p>{@code lastAcceptedAt} takes the later of the two times, so an event that a worker happens
     * to apply after a newer one cannot make the source look less fresh than it is. Acceptance
     * always leaves the source online, which is how a stale source recovers.
     */
    SourceSnapshot accept(Instant receivedAt) {
        Objects.requireNonNull(receivedAt, "receivedAt");
        Instant advanced = receivedAt.isAfter(lastAcceptedAt) ? receivedAt : lastAcceptedAt;
        return new SourceSnapshot(
                sourceId,
                SourceStatus.ONLINE,
                firstAcceptedAt,
                advanced,
                acceptedEventCount + 1,
                processedEventCount + 1);
    }

    /** The same state marked stale. Counters and timestamps are untouched. */
    SourceSnapshot markStale() {
        return new SourceSnapshot(
                sourceId,
                SourceStatus.STALE,
                firstAcceptedAt,
                lastAcceptedAt,
                acceptedEventCount,
                processedEventCount);
    }

    /**
     * Whether this source has been silent for at least {@code staleAfter} as of {@code now}.
     *
     * <p>Exact equality counts as silent, matching {@code now - lastAcceptedAt >= staleAfter}.
     */
    boolean silentFor(Instant now, Duration staleAfter) {
        return Duration.between(lastAcceptedAt, now).compareTo(staleAfter) >= 0;
    }
}
