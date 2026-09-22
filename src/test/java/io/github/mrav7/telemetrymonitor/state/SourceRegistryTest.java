package io.github.mrav7.telemetrymonitor.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.github.mrav7.telemetrymonitor.protocol.ConnectionContext;
import io.github.mrav7.telemetrymonitor.protocol.TelemetryEnvelope;
import io.github.mrav7.telemetrymonitor.protocol.TelemetryEvent;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.LoggerFactory;

/**
 * Covers the source registry: the state one accepted event produces, the global source bound, and
 * the concurrency the registry exists to survive.
 *
 * <p>Time is supplied explicitly everywhere, so the staleness threshold is exercised exactly —
 * including its boundary — without a single wall-clock wait. The concurrent tests use a start gate
 * so the racing operations really do overlap, and assert on externally meaningful invariants rather
 * than on any particular thread ordering.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class SourceRegistryTest {

    private static final Instant T0 = Instant.parse("2026-09-21T18:15:42Z");
    private static final Duration STALE_AFTER = Duration.ofSeconds(30);
    private static final long BOUNDED_WAIT_SECONDS = 30;

    private final ExecutorService threads = Executors.newCachedThreadPool();

    @AfterEach
    void tearDown() {
        threads.shutdownNow();
    }

    private static TelemetryEnvelope envelope(String sourceId, Instant receivedAt) {
        return envelope(sourceId, receivedAt, receivedAt);
    }

    /** An envelope whose declared time is deliberately unrelated to its reception time. */
    private static TelemetryEnvelope envelope(
            String sourceId, Instant receivedAt, Instant occurredAt) {
        return new TelemetryEnvelope(
                new TelemetryEvent(1, sourceId, occurredAt, "temperature", 18.72),
                receivedAt,
                new ConnectionContext(1, "/127.0.0.1:50000"));
    }

    private static SourceSnapshot snapshotOf(SourceRegistry registry, String sourceId) {
        return registry
                .snapshot(sourceId)
                .orElseThrow(() -> new AssertionError("no snapshot for " + sourceId));
    }

    /** Runs every task at once and fails the test if any of them does. */
    private void runTogether(List<Runnable> tasks) throws Exception {
        CyclicBarrier startGate = new CyclicBarrier(tasks.size());
        List<Future<?>> running = new ArrayList<>(tasks.size());
        for (Runnable task : tasks) {
            running.add(
                    threads.submit(
                            () -> {
                                startGate.await(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS);
                                task.run();
                                return null;
                            }));
        }
        for (Future<?> future : running) {
            future.get(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS);
        }
    }

    @Nested
    @DisplayName("source state")
    class State {

        private final SourceRegistry registry = new SourceRegistry(10);

        @Test
        @DisplayName("the first accepted event admits the source as online")
        void firstEventAdmitsTheSource() {
            assertEquals(SourceUpdate.ADMITTED, registry.apply(envelope("source-01", T0)));

            SourceSnapshot snapshot = snapshotOf(registry, "source-01");
            assertEquals("source-01", snapshot.sourceId());
            assertEquals(SourceStatus.ONLINE, snapshot.status());
            assertEquals(T0, snapshot.firstAcceptedAt());
            assertEquals(T0, snapshot.lastAcceptedAt());
            assertEquals(1, snapshot.acceptedEventCount());
            assertEquals(1, snapshot.processedEventCount());
            assertEquals(1, registry.knownSourceCount());
        }

        @Test
        @DisplayName("an unknown source has no snapshot at all, rather than a third status")
        void unknownSourceIsAbsent() {
            assertTrue(registry.snapshot("never-seen").isEmpty());
            assertEquals(0, registry.knownSourceCount());
        }

        @Test
        @DisplayName("a further event counts, advances the reception time and keeps the first one")
        void furtherEventUpdatesTheSource() {
            registry.apply(envelope("source-01", T0));

            assertEquals(
                    SourceUpdate.UPDATED, registry.apply(envelope("source-01", T0.plusSeconds(5))));

            SourceSnapshot snapshot = snapshotOf(registry, "source-01");
            assertEquals(T0, snapshot.firstAcceptedAt());
            assertEquals(T0.plusSeconds(5), snapshot.lastAcceptedAt());
            assertEquals(2, snapshot.acceptedEventCount());
            assertEquals(2, snapshot.processedEventCount());
            assertEquals(1, registry.knownSourceCount(), "the same source is not registered twice");
        }

        @Test
        @DisplayName("an event applied out of order counts without making the source look older")
        void outOfOrderEventDoesNotRegressFreshness() {
            registry.apply(envelope("source-01", T0));
            registry.apply(envelope("source-01", T0.plusSeconds(20)));

            registry.apply(envelope("source-01", T0.plusSeconds(10)));

            SourceSnapshot snapshot = snapshotOf(registry, "source-01");
            assertEquals(
                    T0.plusSeconds(20),
                    snapshot.lastAcceptedAt(),
                    "a late-applied older event must not move the source backwards");
            assertEquals(3, snapshot.acceptedEventCount(), "it is still an accepted event");
            assertEquals(3, snapshot.processedEventCount());
        }

        @Test
        @DisplayName("the time declared by the source has no effect on its liveness")
        void declaredTimeIsNotALivenessSignal() {
            // Reception is old, but the source claims the event just happened.
            registry.apply(envelope("source-01", T0, T0.plusSeconds(3600)));

            assertEquals(T0, snapshotOf(registry, "source-01").lastAcceptedAt());
            assertEquals(1, registry.markStaleSources(T0.plus(STALE_AFTER), STALE_AFTER));
            assertEquals(SourceStatus.STALE, snapshotOf(registry, "source-01").status());
        }

        @Test
        @DisplayName("sources are kept apart")
        void sourcesAreIndependent() {
            registry.apply(envelope("source-01", T0));
            registry.apply(envelope("source-02", T0.plusSeconds(1)));
            registry.apply(envelope("source-02", T0.plusSeconds(2)));

            assertEquals(1, snapshotOf(registry, "source-01").acceptedEventCount());
            assertEquals(2, snapshotOf(registry, "source-02").acceptedEventCount());
            assertEquals(2, registry.knownSourceCount());
        }

        @Test
        @DisplayName("the registry needs a positive source bound")
        void sourceBoundMustBePositive() {
            assertThrows(IllegalArgumentException.class, () -> new SourceRegistry(0));
            assertThrows(IllegalArgumentException.class, () -> new SourceRegistry(-1));
        }
    }

    @Nested
    @DisplayName("staleness")
    class Staleness {

        private final SourceRegistry registry = new SourceRegistry(10);

        @Test
        @DisplayName("a source stays online until the threshold is reached exactly")
        void thresholdIsInclusive() {
            registry.apply(envelope("source-01", T0));

            assertEquals(0, registry.markStaleSources(T0.plus(STALE_AFTER).minusNanos(1), STALE_AFTER));
            assertEquals(SourceStatus.ONLINE, snapshotOf(registry, "source-01").status());

            assertEquals(1, registry.markStaleSources(T0.plus(STALE_AFTER), STALE_AFTER));
            assertEquals(SourceStatus.STALE, snapshotOf(registry, "source-01").status());
        }

        @Test
        @DisplayName("going stale changes nothing but the status")
        void goingStalePreservesCountersAndTimes() {
            registry.apply(envelope("source-01", T0));
            registry.apply(envelope("source-01", T0.plusSeconds(1)));
            registry.markStaleSources(T0.plusSeconds(1).plus(STALE_AFTER), STALE_AFTER);

            SourceSnapshot snapshot = snapshotOf(registry, "source-01");
            assertEquals(T0, snapshot.firstAcceptedAt());
            assertEquals(T0.plusSeconds(1), snapshot.lastAcceptedAt());
            assertEquals(2, snapshot.acceptedEventCount());
            assertEquals(2, snapshot.processedEventCount());
        }

        @Test
        @DisplayName("an already stale source is not transitioned again")
        void staleSourceIsNotTransitionedTwice() {
            registry.apply(envelope("source-01", T0));
            assertEquals(1, registry.markStaleSources(T0.plus(STALE_AFTER), STALE_AFTER));

            assertEquals(0, registry.markStaleSources(T0.plus(STALE_AFTER).plusSeconds(60), STALE_AFTER));
            assertEquals(SourceStatus.STALE, snapshotOf(registry, "source-01").status());
        }

        @Test
        @DisplayName("accepted telemetry brings a stale source back online")
        void acceptedTelemetryRecoversTheSource() {
            registry.apply(envelope("source-01", T0));
            registry.markStaleSources(T0.plus(STALE_AFTER), STALE_AFTER);

            Instant recovery = T0.plus(STALE_AFTER).plusSeconds(5);
            assertEquals(SourceUpdate.RECOVERED, registry.apply(envelope("source-01", recovery)));

            SourceSnapshot snapshot = snapshotOf(registry, "source-01");
            assertEquals(SourceStatus.ONLINE, snapshot.status());
            assertEquals(recovery, snapshot.lastAcceptedAt());
            assertEquals(T0, snapshot.firstAcceptedAt(), "recovery is not a new admission");
            assertEquals(2, snapshot.acceptedEventCount());
            assertEquals(2, snapshot.processedEventCount());
            assertEquals(1, registry.knownSourceCount());
        }

        @Test
        @DisplayName("a late event recovers the source without making it look less fresh")
        void outOfOrderRecoveryKeepsTheLaterTime() {
            Instant t20 = T0.plusSeconds(20);
            Instant t10 = T0.plusSeconds(10);
            registry.apply(envelope("source-01", T0));
            registry.apply(envelope("source-01", t20));
            registry.markStaleSources(t20.plus(STALE_AFTER), STALE_AFTER);
            assertEquals(SourceStatus.STALE, snapshotOf(registry, "source-01").status());
            assertEquals(t20, snapshotOf(registry, "source-01").lastAcceptedAt());

            // An event that reached the monitor before the one already applied. It is a valid
            // accepted event, so it recovers the source and counts — but it cannot rewind it.
            SourceUpdate outcome = registry.apply(envelope("source-01", t10));

            assertEquals(SourceUpdate.RECOVERED, outcome);
            SourceSnapshot snapshot = snapshotOf(registry, "source-01");
            assertEquals(SourceStatus.ONLINE, snapshot.status());
            assertEquals(
                    t20,
                    snapshot.lastAcceptedAt(),
                    "the later reception time must survive a late event");
            assertEquals(T0, snapshot.firstAcceptedAt());
            assertEquals(3, snapshot.acceptedEventCount());
            assertEquals(3, snapshot.processedEventCount());
        }

        @Test
        @DisplayName("the recovery record reports the freshness the transition actually produced")
        void recoveryIsReportedWithTheEffectiveTime() {
            Instant t20 = T0.plusSeconds(20);
            Instant t10 = T0.plusSeconds(10);
            registry.apply(envelope("source-01", T0));
            registry.apply(envelope("source-01", t20));
            registry.markStaleSources(t20.plus(STALE_AFTER), STALE_AFTER);

            List<String> records;
            try (CapturedLogs logs = new CapturedLogs(SourceRegistry.class)) {
                registry.apply(envelope("source-01", t10));
                records = logs.messages();
            }

            String recovery =
                    records.stream()
                            .filter(record -> record.contains("event=source_online"))
                            .findFirst()
                            .orElseThrow(
                                    () -> new AssertionError("no recovery record in " + records));
            assertTrue(
                    recovery.contains("last_accepted_at=" + t20),
                    "the record must report the effective time, but was: " + recovery);
            assertFalse(
                    recovery.contains("last_accepted_at=" + t10),
                    "the record must not report the incoming time, but was: " + recovery);
        }

        @Test
        @DisplayName("a recovered source can go stale again")
        void recoveredSourceCanGoStaleAgain() {
            registry.apply(envelope("source-01", T0));
            registry.markStaleSources(T0.plus(STALE_AFTER), STALE_AFTER);
            Instant recovery = T0.plus(STALE_AFTER).plusSeconds(5);
            registry.apply(envelope("source-01", recovery));

            assertEquals(1, registry.markStaleSources(recovery.plus(STALE_AFTER), STALE_AFTER));
            assertEquals(SourceStatus.STALE, snapshotOf(registry, "source-01").status());
        }

        @Test
        @DisplayName("only the silent sources are marked, and only they")
        void onlySilentSourcesAreMarked() {
            registry.apply(envelope("quiet", T0));
            registry.apply(envelope("talkative", T0.plus(STALE_AFTER)));

            assertEquals(1, registry.markStaleSources(T0.plus(STALE_AFTER), STALE_AFTER));
            assertEquals(SourceStatus.STALE, snapshotOf(registry, "quiet").status());
            assertEquals(SourceStatus.ONLINE, snapshotOf(registry, "talkative").status());
        }
    }

    @Nested
    @DisplayName("source capacity")
    class Capacity {

        @Test
        @DisplayName("an unknown source is refused once the registry is full, and known ones are not")
        void unknownSourcesAreRefusedAtCapacity() {
            SourceRegistry registry = new SourceRegistry(2);

            assertEquals(SourceUpdate.ADMITTED, registry.apply(envelope("source-a", T0)));
            assertEquals(SourceUpdate.ADMITTED, registry.apply(envelope("source-b", T0)));
            assertEquals(
                    SourceUpdate.REJECTED_AT_CAPACITY, registry.apply(envelope("source-c", T0)));

            assertEquals(2, registry.knownSourceCount());
            assertTrue(registry.snapshot("source-c").isEmpty(), "a refused source is not created");

            assertEquals(
                    SourceUpdate.UPDATED, registry.apply(envelope("source-a", T0.plusSeconds(1))));
            assertEquals(
                    SourceUpdate.UPDATED, registry.apply(envelope("source-b", T0.plusSeconds(1))));
            assertEquals(2, snapshotOf(registry, "source-a").acceptedEventCount());
            assertEquals(2, snapshotOf(registry, "source-b").acceptedEventCount());
            assertEquals(2, registry.knownSourceCount(), "nothing was evicted to make room");
        }

        @Test
        @DisplayName("a refused event leaves every known source exactly as it was")
        void refusedEventChangesNothing() {
            SourceRegistry registry = new SourceRegistry(1);
            registry.apply(envelope("source-a", T0));
            SourceSnapshot before = snapshotOf(registry, "source-a");

            registry.apply(envelope("source-b", T0.plusSeconds(1)));

            assertEquals(before, snapshotOf(registry, "source-a"));
        }

        @Test
        @DisplayName("a stale source keeps its slot")
        void staleSourceStillCountsAgainstCapacity() {
            SourceRegistry registry = new SourceRegistry(1);
            registry.apply(envelope("source-a", T0));
            registry.markStaleSources(T0.plus(STALE_AFTER), STALE_AFTER);

            assertEquals(
                    SourceUpdate.REJECTED_AT_CAPACITY,
                    registry.apply(envelope("source-b", T0.plus(STALE_AFTER))),
                    "a stale source is still registered, so the slot is still taken");
            assertEquals(1, registry.knownSourceCount());
            assertEquals(SourceStatus.STALE, snapshotOf(registry, "source-a").status());
        }

        @Test
        @DisplayName("simultaneous unknown sources fill the registry exactly, never past the bound")
        void simultaneousAdmissionCannotExceedTheBound() throws Exception {
            int capacity = 4;
            int attempts = 64;
            SourceRegistry registry = new SourceRegistry(capacity);
            AtomicInteger admitted = new AtomicInteger();
            AtomicInteger rejected = new AtomicInteger();

            List<Runnable> tasks = new ArrayList<>();
            for (int attempt = 0; attempt < attempts; attempt++) {
                String sourceId = "source-" + attempt;
                tasks.add(
                        () -> {
                            SourceUpdate outcome = registry.apply(envelope(sourceId, T0));
                            (outcome == SourceUpdate.ADMITTED ? admitted : rejected)
                                    .incrementAndGet();
                        });
            }
            runTogether(tasks);

            assertEquals(capacity, registry.knownSourceCount(), "the bound must hold exactly");
            assertEquals(capacity, admitted.get(), "every free slot must be used");
            assertEquals(attempts - capacity, rejected.get());
            for (SourceSnapshot snapshot : registry.snapshots()) {
                assertEquals(SourceStatus.ONLINE, snapshot.status());
                assertEquals(1, snapshot.acceptedEventCount(), "an admitted source is coherent");
                assertEquals(T0, snapshot.firstAcceptedAt());
            }

            // No slot was leaked by the losers: the winners keep working, and there is still no room.
            for (SourceSnapshot snapshot : registry.snapshots()) {
                assertEquals(
                        SourceUpdate.UPDATED,
                        registry.apply(envelope(snapshot.sourceId(), T0.plusSeconds(1))));
            }
            assertEquals(
                    SourceUpdate.REJECTED_AT_CAPACITY, registry.apply(envelope("late-comer", T0)));
            assertEquals(capacity, registry.knownSourceCount());
        }

        @Test
        @DisplayName("the same unknown source arriving at once is admitted once and counts each event")
        void sameUnknownSourceRacesIntoOneCoherentSource() throws Exception {
            int events = 32;
            SourceRegistry registry = new SourceRegistry(10);
            AtomicInteger admissions = new AtomicInteger();

            List<Runnable> tasks = new ArrayList<>();
            for (int event = 0; event < events; event++) {
                Instant receivedAt = T0.plusMillis(event);
                tasks.add(
                        () -> {
                            if (registry.apply(envelope("source-01", receivedAt))
                                    == SourceUpdate.ADMITTED) {
                                admissions.incrementAndGet();
                            }
                        });
            }
            runTogether(tasks);

            assertEquals(1, admissions.get(), "the source may only be admitted once");
            assertEquals(1, registry.knownSourceCount());
            SourceSnapshot snapshot = snapshotOf(registry, "source-01");
            assertEquals(events, snapshot.acceptedEventCount(), "no event may be lost");
            assertEquals(events, snapshot.processedEventCount());
            assertEquals(T0.plusMillis(events - 1L), snapshot.lastAcceptedAt());
        }
    }

    @Nested
    @DisplayName("concurrent updates")
    class ConcurrentUpdates {

        @Test
        @DisplayName("concurrent events for one source lose no count and settle on the latest time")
        void sameSourceUpdatesAreNotLost() throws Exception {
            int events = 200;
            SourceRegistry registry = new SourceRegistry(10);
            registry.apply(envelope("source-01", T0));

            List<Runnable> tasks = new ArrayList<>();
            for (int event = 1; event <= events; event++) {
                // Deliberately out of order: the thread that runs last is not the latest event.
                Instant receivedAt = T0.plusMillis(event % 2 == 0 ? event : events + 1L - event);
                tasks.add(() -> registry.apply(envelope("source-01", receivedAt)));
            }
            runTogether(tasks);

            SourceSnapshot snapshot = snapshotOf(registry, "source-01");
            assertEquals(
                    events + 1L,
                    snapshot.acceptedEventCount(),
                    "every applied event must be counted exactly once");
            assertEquals(events + 1L, snapshot.processedEventCount());
            assertEquals(
                    T0.plusMillis(events),
                    snapshot.lastAcceptedAt(),
                    "the source must end as fresh as its newest event");
            assertEquals(T0, snapshot.firstAcceptedAt());
            assertEquals(SourceStatus.ONLINE, snapshot.status());
        }

        @Test
        @DisplayName("concurrent events across sources do not corrupt one another")
        void differentSourcesDoNotInterfere() throws Exception {
            int sources = 8;
            int eventsPerSource = 50;
            SourceRegistry registry = new SourceRegistry(sources);

            List<Runnable> tasks = new ArrayList<>();
            for (int source = 0; source < sources; source++) {
                String sourceId = "source-" + source;
                for (int event = 1; event <= eventsPerSource; event++) {
                    Instant receivedAt = T0.plusMillis(event);
                    tasks.add(() -> registry.apply(envelope(sourceId, receivedAt)));
                }
            }
            runTogether(tasks);

            assertEquals(sources, registry.knownSourceCount());
            Set<String> seen =
                    registry.snapshots().stream()
                            .map(SourceSnapshot::sourceId)
                            .collect(Collectors.toSet());
            assertEquals(sources, seen.size());
            for (SourceSnapshot snapshot : registry.snapshots()) {
                assertEquals(
                        eventsPerSource,
                        snapshot.acceptedEventCount(),
                        "each source must count exactly its own events: " + snapshot.sourceId());
                assertEquals(eventsPerSource, snapshot.processedEventCount());
                assertEquals(T0.plusMillis(eventsPerSource), snapshot.lastAcceptedAt());
                // Which of the racing events admitted the source is not decided by this test, so
                // the assertion is that the admission time is one of them and never later than the
                // freshest one, rather than a particular value.
                assertTrue(
                        !snapshot.firstAcceptedAt().isBefore(T0.plusMillis(1))
                                && !snapshot.firstAcceptedAt().isAfter(snapshot.lastAcceptedAt()),
                        "unexpected admission time for " + snapshot.sourceId());
                assertEquals(SourceStatus.ONLINE, snapshot.status());
            }
        }

        @Test
        @DisplayName("a stale check running against fresh telemetry cannot undo the acceptance")
        void staleCheckCannotOverwriteAcceptedTelemetry() throws Exception {
            int rounds = 300;

            for (int round = 0; round < rounds; round++) {
                // Each round starts from a source that was last accepted staleAfter ago, so the
                // evaluation below would mark it stale on the state it starts from.
                Instant silentSince = T0.plusSeconds(round);
                Instant now = silentSince.plus(STALE_AFTER);
                SourceRegistry perRound = new SourceRegistry(10);
                perRound.apply(envelope("source-01", silentSince));

                CountDownLatch start = new CountDownLatch(1);
                Future<?> telemetry =
                        threads.submit(
                                () -> {
                                    start.await(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS);
                                    // Reception at `now` is fresh for an evaluation at `now`.
                                    return perRound.apply(envelope("source-01", now));
                                });
                Future<?> staleCheck =
                        threads.submit(
                                () -> {
                                    start.await(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS);
                                    return perRound.markStaleSources(now, STALE_AFTER);
                                });
                start.countDown();
                telemetry.get(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS);
                staleCheck.get(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS);

                SourceSnapshot snapshot = snapshotOf(perRound, "source-01");
                // Whichever order the two ran in, the only consistent outcome is online: an
                // evaluation before the event finds a stale source but the event then recovers it,
                // and an evaluation after the event finds a source that is not silent at all. A
                // stale result here would mean the check wrote a decision taken on state it had
                // already stopped holding.
                assertEquals(
                        SourceStatus.ONLINE,
                        snapshot.status(),
                        "round " + round + ": a stale check must not overwrite fresh telemetry");
                assertEquals(now, snapshot.lastAcceptedAt());
                assertEquals(2, snapshot.acceptedEventCount());
            }
        }

        @Test
        @DisplayName("telemetry and stale checks running together keep every invariant")
        void mixedTrafficKeepsInvariants() throws Exception {
            int events = 200;
            int checks = 50;
            SourceRegistry registry = new SourceRegistry(4);
            List<Runnable> tasks = new ArrayList<>();

            for (int event = 1; event <= events; event++) {
                String sourceId = "source-" + (event % 4);
                Instant receivedAt = T0.plusMillis(event);
                tasks.add(() -> registry.apply(envelope(sourceId, receivedAt)));
            }
            for (int check = 0; check < checks; check++) {
                Instant now = T0.plusMillis(check).plus(STALE_AFTER);
                tasks.add(() -> registry.markStaleSources(now, STALE_AFTER));
            }
            runTogether(tasks);

            assertEquals(4, registry.knownSourceCount());
            long counted = 0;
            for (SourceSnapshot snapshot : registry.snapshots()) {
                assertTrue(
                        snapshot.processedEventCount() <= snapshot.acceptedEventCount(),
                        "processed must never exceed accepted");
                assertTrue(
                        !snapshot.lastAcceptedAt().isBefore(snapshot.firstAcceptedAt()),
                        "freshness must never fall behind admission");
                counted += snapshot.acceptedEventCount();
            }
            assertEquals(events, counted, "no event may be lost between sources");
        }

        @Test
        @DisplayName("a handed-out snapshot is never mutated by later events")
        void snapshotsAreStable() {
            SourceRegistry registry = new SourceRegistry(10);
            registry.apply(envelope("source-01", T0));
            SourceSnapshot taken = snapshotOf(registry, "source-01");

            registry.apply(envelope("source-01", T0.plusSeconds(1)));

            assertEquals(1, taken.acceptedEventCount());
            assertEquals(T0, taken.lastAcceptedAt());
            assertEquals(2, snapshotOf(registry, "source-01").acceptedEventCount());
        }
    }

    /**
     * Captures what the registry logs, so an operational record can be asserted on.
     *
     * <p>This is the one place the suite touches the logging binding directly. It is deliberately
     * small: attaching an appender for the duration of a test needs no dependency and no production
     * API added for testing. The level is set explicitly so that a {@code TM_LOG_LEVEL} in the
     * developer's or the runner's environment cannot decide whether the assertion holds, and the
     * previous level is restored afterwards.
     */
    private static final class CapturedLogs implements AutoCloseable {

        private final ch.qos.logback.classic.Logger logger;
        private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
        private final Level previousLevel;

        private CapturedLogs(Class<?> source) {
            this.logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(source);
            this.previousLevel = logger.getLevel();
            logger.setLevel(Level.INFO);
            appender.start();
            logger.addAppender(appender);
        }

        /** The formatted records emitted while this capture was open. */
        List<String> messages() {
            return appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
        }

        @Override
        public void close() {
            logger.detachAppender(appender);
            appender.stop();
            logger.setLevel(previousLevel);
        }
    }
}
