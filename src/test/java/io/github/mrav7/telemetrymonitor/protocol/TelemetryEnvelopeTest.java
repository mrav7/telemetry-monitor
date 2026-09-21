package io.github.mrav7.telemetrymonitor.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Covers the separation between the source-declared {@code occurredAt} and the monitor-generated
 * {@code receivedAt}, using a fixed clock so the expectation is exact rather than approximate.
 */
class TelemetryEnvelopeTest {

    private static final Instant MONITOR_NOW = Instant.parse("2026-09-21T18:15:42.123Z");
    private static final Clock FIXED = Clock.fixed(MONITOR_NOW, ZoneOffset.UTC);

    private static TelemetryEvent eventOccurringAt(String occurredAt) {
        return new TelemetryEvent(1, "source-01", Instant.parse(occurredAt), "temperature", 18.72);
    }

    @Test
    @DisplayName("receivedAt comes from the supplied clock")
    void receivedAtComesFromTheClock() {
        TelemetryEnvelope envelope =
                TelemetryEnvelope.receivedNow(eventOccurringAt("2026-09-21T18:15:42.000Z"), FIXED);

        assertEquals(MONITOR_NOW, envelope.receivedAt());
        assertEquals(FIXED.instant(), envelope.receivedAt());
    }

    @Test
    @DisplayName("occurredAt is preserved and never overwritten by monitor time")
    void occurredAtSurvivesEnvelopeCreation() {
        Instant declared = Instant.parse("2001-02-03T04:05:06Z");
        TelemetryEnvelope envelope =
                TelemetryEnvelope.receivedNow(eventOccurringAt(declared.toString()), FIXED);

        assertEquals(declared, envelope.event().occurredAt());
        assertNotEquals(envelope.event().occurredAt(), envelope.receivedAt());
    }

    @Test
    @DisplayName("a far-future occurredAt does not influence receivedAt")
    void wildlyDivergentOccurredAtIsIgnoredForReceivedAt() {
        TelemetryEnvelope envelope =
                TelemetryEnvelope.receivedNow(eventOccurringAt("2999-12-31T23:59:59Z"), FIXED);

        assertEquals(MONITOR_NOW, envelope.receivedAt());
    }

    @Test
    @DisplayName("two envelopes from the same fixed clock agree")
    void sameClockYieldsSameReceivedAt() {
        TelemetryEvent event = eventOccurringAt("2026-09-21T18:15:42.000Z");

        assertEquals(
                TelemetryEnvelope.receivedNow(event, FIXED).receivedAt(),
                TelemetryEnvelope.receivedNow(event, FIXED).receivedAt());
    }

    @Test
    @DisplayName("a non-finite value cannot be held by a validated event")
    void eventRejectsNonFiniteValue() {
        Instant occurredAt = Instant.parse("2026-09-21T18:15:42Z");

        assertThrows(
                IllegalArgumentException.class,
                () -> new TelemetryEvent(1, "s", occurredAt, "m", Double.NaN));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TelemetryEvent(1, "s", occurredAt, "m", Double.POSITIVE_INFINITY));
    }
}
