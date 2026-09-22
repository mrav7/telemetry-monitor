package io.github.mrav7.telemetrymonitor.state;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.mrav7.telemetrymonitor.configuration.ConfigurationLoader;
import io.github.mrav7.telemetrymonitor.configuration.MonitorConfiguration;
import io.github.mrav7.telemetrymonitor.lifecycle.ShutdownDeadline;
import io.github.mrav7.telemetrymonitor.protocol.ConnectionContext;
import io.github.mrav7.telemetrymonitor.protocol.TelemetryEnvelope;
import io.github.mrav7.telemetrymonitor.protocol.TelemetryEvent;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Covers the scheduled side of monitoring: that the production scheduler really does drive the
 * registry's staleness evaluation, at the configured interval, and that it releases its thread.
 *
 * <p>The threshold itself is exercised exhaustively against the registry in {@code
 * SourceRegistryTest}. What matters here is the wiring, so the monitor's clock is the test's to
 * move: a source becomes stale because the clock was moved past the threshold, never because the
 * test waited out a real one.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class StaleMonitorTest {

    private static final Instant T0 = Instant.parse("2026-09-21T18:15:42Z");
    private static final Duration STALE_AFTER = Duration.ofSeconds(30);

    /** The shortest interval the configuration contract allows, so the test never waits long. */
    private static final Duration CHECK_INTERVAL = Duration.ofSeconds(1);

    private static final Duration BOUNDED_WAIT = Duration.ofSeconds(30);

    private StaleMonitor monitor;

    @AfterEach
    void tearDown() {
        if (monitor != null) {
            monitor.close();
        }
    }

    /** A clock the test moves by hand, so no wall-clock time has to pass for a threshold to. */
    private static final class MovableClock extends Clock {

        private final AtomicReference<Instant> now;

        private MovableClock(Instant start) {
            this.now = new AtomicReference<>(start);
        }

        void set(Instant instant) {
            now.set(instant);
        }

        @Override
        public Instant instant() {
            return now.get();
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }

    private static TelemetryEnvelope envelope(String sourceId, Instant receivedAt) {
        return new TelemetryEnvelope(
                new TelemetryEvent(1, sourceId, receivedAt, "temperature", 18.72),
                receivedAt,
                new ConnectionContext(1, "/127.0.0.1:50000"));
    }

    @Test
    @DisplayName("the scheduler evaluates staleness periodically, without being asked once")
    void scheduledEvaluationMarksSilentSources() {
        MovableClock clock = new MovableClock(T0);
        SourceRegistry registry = new SourceRegistry(10);
        registry.apply(envelope("source-01", T0));

        monitor = new StaleMonitor(STALE_AFTER, CHECK_INTERVAL, clock, registry);
        monitor.start();

        // Several intervals pass with the clock still inside the threshold.
        await().pollDelay(CHECK_INTERVAL.multipliedBy(2))
                .atMost(BOUNDED_WAIT)
                .until(
                        () ->
                                registry.snapshot("source-01").orElseThrow().status()
                                        == SourceStatus.ONLINE);

        clock.set(T0.plus(STALE_AFTER));

        await().atMost(BOUNDED_WAIT)
                .until(
                        () ->
                                registry.snapshot("source-01").orElseThrow().status()
                                        == SourceStatus.STALE);
    }

    @Test
    @DisplayName("a source that reports again goes back online and stays there")
    void acceptedTelemetryRecoversBetweenScheduledChecks() {
        MovableClock clock = new MovableClock(T0);
        SourceRegistry registry = new SourceRegistry(10);
        registry.apply(envelope("source-01", T0));

        monitor = new StaleMonitor(STALE_AFTER, CHECK_INTERVAL, clock, registry);
        monitor.start();

        clock.set(T0.plus(STALE_AFTER));
        await().atMost(BOUNDED_WAIT)
                .until(
                        () ->
                                registry.snapshot("source-01").orElseThrow().status()
                                        == SourceStatus.STALE);

        Instant recovery = T0.plus(STALE_AFTER);
        registry.apply(envelope("source-01", recovery));
        assertEquals(SourceStatus.ONLINE, registry.snapshot("source-01").orElseThrow().status());

        // The schedule keeps running; while the clock stays inside the threshold it must leave the
        // recovered source alone.
        await().pollDelay(CHECK_INTERVAL.multipliedBy(2))
                .atMost(BOUNDED_WAIT)
                .until(
                        () ->
                                registry.snapshot("source-01").orElseThrow().status()
                                        == SourceStatus.ONLINE);
    }

    @Test
    @DisplayName("every silent source is evaluated, and only the silent ones")
    void allSilentSourcesAreEvaluated() {
        MovableClock clock = new MovableClock(T0);
        SourceRegistry registry = new SourceRegistry(10);
        registry.apply(envelope("quiet-a", T0));
        registry.apply(envelope("quiet-b", T0));
        registry.apply(envelope("talkative", T0.plus(STALE_AFTER)));

        monitor = new StaleMonitor(STALE_AFTER, CHECK_INTERVAL, clock, registry);
        monitor.start();
        clock.set(T0.plus(STALE_AFTER));

        await().atMost(BOUNDED_WAIT)
                .until(
                        () ->
                                registry.snapshots().stream()
                                                .filter(s -> s.status() == SourceStatus.STALE)
                                                .count()
                                        == 2);
        Set<String> stale =
                registry.snapshots().stream()
                        .filter(snapshot -> snapshot.status() == SourceStatus.STALE)
                        .map(SourceSnapshot::sourceId)
                        .collect(Collectors.toSet());
        assertEquals(Set.of("quiet-a", "quiet-b"), stale);
    }

    @Test
    @DisplayName("the configured interval is what the monitor is scheduled at")
    void configuredIntervalIsUsed() throws Exception {
        MonitorConfiguration configuration =
                ConfigurationLoader.load(
                        Map.of(
                                ConfigurationLoader.STALE_AFTER_SECONDS, "2",
                                ConfigurationLoader.STALE_CHECK_INTERVAL_SECONDS, "1"));
        MovableClock clock = new MovableClock(T0);
        SourceRegistry registry = new SourceRegistry(10);
        registry.apply(envelope("source-01", T0));

        monitor = new StaleMonitor(configuration, clock, registry);
        monitor.start();
        clock.set(T0.plusSeconds(2));

        // Both values come from the configuration. The bound is deliberately shorter than the
        // default interval of five seconds: had the monitor been scheduled at anything but the
        // second it was configured with, the first evaluation would arrive too late for this.
        await().atMost(Duration.ofSeconds(4))
                .until(
                        () ->
                                registry.snapshot("source-01").orElseThrow().status()
                                        == SourceStatus.STALE);
    }

    @Test
    @DisplayName("closing stops the schedule and leaves no thread behind")
    void closingReleasesTheSchedulerThread() {
        MovableClock clock = new MovableClock(T0);
        SourceRegistry registry = new SourceRegistry(10);
        registry.apply(envelope("source-01", T0));

        monitor = new StaleMonitor(STALE_AFTER, CHECK_INTERVAL, clock, registry);
        monitor.start();
        await().atMost(BOUNDED_WAIT).until(() -> schedulerThreadExists());

        monitor.close();
        monitor.close();

        await().atMost(BOUNDED_WAIT).until(() -> !schedulerThreadExists());

        // The schedule is gone, so a clock past the threshold no longer changes anything.
        clock.set(T0.plus(STALE_AFTER).plusSeconds(60));
        assertTrue(
                Thread.getAllStackTraces().keySet().stream()
                        .noneMatch(thread -> thread.getName().startsWith("tm-stale-monitor")));
        assertEquals(SourceStatus.ONLINE, registry.snapshot("source-01").orElseThrow().status());
    }

    @Test
    @DisplayName("coordinated stop terminates the scheduler within the shared deadline")
    void coordinatedStopTerminatesScheduler() throws Exception {
        MovableClock clock = new MovableClock(T0);
        SourceRegistry registry = new SourceRegistry(10);
        registry.apply(envelope("source-01", T0));
        monitor = new StaleMonitor(STALE_AFTER, CHECK_INTERVAL, clock, registry);
        monitor.start();

        monitor.requestStop();
        assertTrue(monitor.awaitTermination(ShutdownDeadline.start(Duration.ofSeconds(5))));

        clock.set(T0.plus(STALE_AFTER));
        await().pollDelay(CHECK_INTERVAL.multipliedBy(2))
                .atMost(BOUNDED_WAIT)
                .until(
                        () ->
                                registry.snapshot("source-01").orElseThrow().status()
                                        == SourceStatus.ONLINE);
    }

    private static boolean schedulerThreadExists() {
        return Thread.getAllStackTraces().keySet().stream()
                .anyMatch(thread -> thread.getName().startsWith("tm-stale-monitor"));
    }
}
