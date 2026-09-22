package io.github.mrav7.telemetrymonitor.protocol;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;

/**
 * A validated event paired with the time the monitor received it.
 *
 * <p>{@code receivedAt} comes from the monitor's clock and is the only timestamp used for liveness
 * decisions. It is deliberately independent of {@link TelemetryEvent#occurredAt()}, which is
 * declared by the remote process and is not treated as trustworthy for that purpose.
 *
 * <p>{@code connection} records where the message arrived from, for diagnostics only. It is
 * deliberately not an identity: the protocol lets one connection carry several sources, so source
 * decisions are made from {@link TelemetryEvent#sourceId()} alone.
 *
 * @param event      the validated telemetry event
 * @param receivedAt monitor-side reception time
 * @param connection the connection the message was read from
 */
public record TelemetryEnvelope(
        TelemetryEvent event, Instant receivedAt, ConnectionContext connection) {

    public TelemetryEnvelope {
        Objects.requireNonNull(event, "event");
        Objects.requireNonNull(receivedAt, "receivedAt");
        Objects.requireNonNull(connection, "connection");
    }

    /** Wraps an event, stamping it with the supplied clock. */
    public static TelemetryEnvelope receivedNow(
            TelemetryEvent event, Clock clock, ConnectionContext connection) {
        Objects.requireNonNull(clock, "clock");
        return new TelemetryEnvelope(event, clock.instant(), connection);
    }
}
