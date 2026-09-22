package io.github.mrav7.telemetrymonitor.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Covers the observable source state itself: the invariants it refuses to be constructed without,
 * and the two transitions it can express. The registry relies on these holding structurally, which
 * is what makes a snapshot safe to hand out while other threads are updating the same source.
 */
class SourceSnapshotTest {

    private static final Instant T0 = Instant.parse("2026-09-21T18:15:42Z");
    private static final Duration STALE_AFTER = Duration.ofSeconds(30);

    @Test
    @DisplayName("an admitted source is online, counted once, and stamped with its reception time")
    void firstAcceptedStartsOnline() {
        SourceSnapshot snapshot = SourceSnapshot.firstAccepted("source-01", T0);

        assertEquals("source-01", snapshot.sourceId());
        assertEquals(SourceStatus.ONLINE, snapshot.status());
        assertEquals(T0, snapshot.firstAcceptedAt());
        assertEquals(T0, snapshot.lastAcceptedAt());
        assertEquals(1, snapshot.acceptedEventCount());
        assertEquals(1, snapshot.processedEventCount());
    }

    @Test
    @DisplayName("acceptance advances the reception time, counts the event and leaves it online")
    void acceptAdvancesState() {
        SourceSnapshot accepted =
                SourceSnapshot.firstAccepted("source-01", T0).markStale().accept(T0.plusSeconds(5));

        assertEquals(SourceStatus.ONLINE, accepted.status());
        assertEquals(T0, accepted.firstAcceptedAt(), "the admission time never changes");
        assertEquals(T0.plusSeconds(5), accepted.lastAcceptedAt());
        assertEquals(2, accepted.acceptedEventCount());
        assertEquals(2, accepted.processedEventCount());
    }

    @Test
    @DisplayName("an older reception time counts the event without moving the source backwards")
    void acceptKeepsTheLaterTime() {
        SourceSnapshot accepted =
                SourceSnapshot.firstAccepted("source-01", T0.plusSeconds(10)).accept(T0);

        assertEquals(T0.plusSeconds(10), accepted.lastAcceptedAt());
        assertEquals(2, accepted.acceptedEventCount());
    }

    @Test
    @DisplayName("marking stale changes only the status")
    void markStaleKeepsEverythingElse() {
        SourceSnapshot online = SourceSnapshot.firstAccepted("source-01", T0).accept(T0.plusSeconds(1));
        SourceSnapshot stale = online.markStale();

        assertEquals(SourceStatus.STALE, stale.status());
        assertEquals(online.firstAcceptedAt(), stale.firstAcceptedAt());
        assertEquals(online.lastAcceptedAt(), stale.lastAcceptedAt());
        assertEquals(online.acceptedEventCount(), stale.acceptedEventCount());
        assertEquals(online.processedEventCount(), stale.processedEventCount());
    }

    @Test
    @DisplayName("silence is measured inclusively: exactly the threshold already counts")
    void silenceIncludesTheThresholdItself() {
        SourceSnapshot snapshot = SourceSnapshot.firstAccepted("source-01", T0);

        assertFalse(snapshot.silentFor(T0.plus(STALE_AFTER).minusNanos(1), STALE_AFTER));
        assertTrue(snapshot.silentFor(T0.plus(STALE_AFTER), STALE_AFTER));
        assertTrue(snapshot.silentFor(T0.plus(STALE_AFTER).plusNanos(1), STALE_AFTER));
    }

    @Test
    @DisplayName("a snapshot cannot exist with processed above accepted")
    void processedCannotExceedAccepted() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new SourceSnapshot("source-01", SourceStatus.ONLINE, T0, T0, 1, 2));
    }

    @Test
    @DisplayName("a snapshot cannot exist with a last acceptance before its first")
    void lastCannotPrecedeFirst() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new SourceSnapshot(
                                "source-01", SourceStatus.ONLINE, T0, T0.minusSeconds(1), 1, 1));
    }

    @Test
    @DisplayName("a snapshot cannot exist with a negative count")
    void countsCannotBeNegative() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new SourceSnapshot("source-01", SourceStatus.ONLINE, T0, T0, -1, -1));
    }

    @Test
    @DisplayName("a snapshot cannot exist without its identifying parts")
    void requiredPartsAreRejectedWhenMissing() {
        assertThrows(
                NullPointerException.class,
                () -> new SourceSnapshot(null, SourceStatus.ONLINE, T0, T0, 1, 1));
        assertThrows(
                NullPointerException.class, () -> new SourceSnapshot("source-01", null, T0, T0, 1, 1));
        assertThrows(
                NullPointerException.class,
                () -> new SourceSnapshot("source-01", SourceStatus.ONLINE, null, T0, 1, 1));
        assertThrows(
                NullPointerException.class,
                () -> new SourceSnapshot("source-01", SourceStatus.ONLINE, T0, null, 1, 1));
    }
}
