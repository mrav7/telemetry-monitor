package io.github.mrav7.telemetrymonitor.state;

import io.github.mrav7.telemetrymonitor.configuration.MonitorConfiguration;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the staleness evaluation on a schedule.
 *
 * <p>This is the only thing in the service that asks the registry to mark sources stale. It holds no
 * source state of its own: it owns a scheduler and a clock, and hands both to the registry, which
 * decides. Nothing here creates a source, brings one back online or touches connections.
 *
 * <p>Time comes from the injected {@link Clock}, which is what lets a test drive the threshold
 * without waiting out a real interval.
 *
 * <p>Ownership: this class creates its executor and shuts it down in {@link #close()}. That is
 * resource termination only — the service-wide shutdown sequence is separate work.
 */
public final class StaleMonitor implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StaleMonitor.class);

    /** Bound on waiting for the scheduler to stop, so runs and tests do not leak its thread. */
    private static final long TERMINATION_TIMEOUT_SECONDS = 5;

    private final SourceRegistry registry;
    private final Clock clock;
    private final Duration staleAfter;
    private final Duration checkInterval;
    private final ScheduledExecutorService scheduler;

    public StaleMonitor(
            MonitorConfiguration configuration, Clock clock, SourceRegistry registry) {
        this(
                Objects.requireNonNull(configuration, "configuration").staleAfter(),
                configuration.staleCheckInterval(),
                clock,
                registry);
    }

    StaleMonitor(
            Duration staleAfter, Duration checkInterval, Clock clock, SourceRegistry registry) {
        this.staleAfter = Objects.requireNonNull(staleAfter, "staleAfter");
        this.checkInterval = Objects.requireNonNull(checkInterval, "checkInterval");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.registry = Objects.requireNonNull(registry, "registry");
        ThreadFactory factory =
                Thread.ofPlatform().name("tm-stale-monitor").daemon(false).factory();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(factory);
    }

    /** Starts periodic evaluation at the configured interval. */
    public void start() {
        scheduler.scheduleAtFixedRate(
                this::evaluate,
                checkInterval.toMillis(),
                checkInterval.toMillis(),
                TimeUnit.MILLISECONDS);
        log.debug(
                "event=stale_monitor_started check_interval_seconds={} stale_after_seconds={}",
                checkInterval.toSeconds(),
                staleAfter.toSeconds());
    }

    /** Evaluates once, as the schedule does. Exposed so a test can drive a single pass. */
    void evaluate() {
        try {
            registry.markStaleSources(clock.instant(), staleAfter);
        } catch (RuntimeException e) {
            // A periodic task that throws is never scheduled again, which would silently end
            // staleness detection for the life of the process. Report and keep the schedule.
            log.error("event=stale_check_failure reason={}", e.toString(), e);
        }
    }

    /** Stops the schedule and releases the executor. Safe to call more than once. */
    @Override
    public void close() {
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.warn(
                        "event=stale_monitor_not_terminated timeout_seconds={}",
                        TERMINATION_TIMEOUT_SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
