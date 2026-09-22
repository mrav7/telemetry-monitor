package io.github.mrav7.telemetrymonitor.network;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.mrav7.telemetrymonitor.configuration.LogLevel;
import io.github.mrav7.telemetrymonitor.configuration.MonitorConfiguration;
import io.github.mrav7.telemetrymonitor.simulator.MalformedCase;
import io.github.mrav7.telemetrymonitor.simulator.SimulatorConfiguration;
import io.github.mrav7.telemetrymonitor.simulator.SimulatorMode;
import io.github.mrav7.telemetrymonitor.simulator.TelemetrySimulator;
import io.github.mrav7.telemetrymonitor.state.SourceSnapshot;
import io.github.mrav7.telemetrymonitor.state.SourceStatus;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 30, unit = TimeUnit.SECONDS)
class TelemetrySimulatorE2ETest {

    private static final Duration EVENTUAL = Duration.ofSeconds(10);
    private static final Duration LONG_STALE_AFTER = Duration.ofSeconds(30);
    private static final Duration CHECK_INTERVAL = Duration.ofMillis(250);

    @Test
    @DisplayName("normal simulator traffic reaches ONLINE source state")
    void normalBecomesOnline() throws Exception {
        try (TelemetryMonitorTestRuntime runtime =
                new TelemetryMonitorTestRuntime(configuration(2, 16, LONG_STALE_AFTER))) {
            new TelemetrySimulator()
                    .run(
                            simulator(
                                    runtime.port(),
                                    "normal-source",
                                    SimulatorMode.NORMAL,
                                    Duration.ofMillis(100),
                                    Duration.ofMillis(600),
                                    0,
                                    null));

            await().atMost(EVENTUAL)
                    .untilAsserted(
                            () -> {
                                SourceSnapshot source = snapshot(runtime, "normal-source");
                                assertEquals(SourceStatus.ONLINE, source.status());
                                assertTrue(source.acceptedEventCount() >= 1);
                                assertTrue(source.processedEventCount() >= 1);
                                assertTrue(
                                        source.processedEventCount()
                                                <= source.acceptedEventCount());
                            });
        }
    }

    @Test
    @DisplayName("a silent connected source becomes STALE and recovers on a new connection")
    void silentBecomesStaleAndRecovers() throws Exception {
        ExecutorService simulatorExecutor = Executors.newSingleThreadExecutor();
        Future<?> silent = null;
        try (TelemetryMonitorTestRuntime runtime =
                new TelemetryMonitorTestRuntime(
                        configuration(2, 8, Duration.ofSeconds(2)))) {
            SimulatorConfiguration silentConfiguration =
                    simulator(
                            runtime.port(),
                            "recovering-source",
                            SimulatorMode.SILENT,
                            Duration.ZERO,
                            Duration.ofSeconds(8),
                            0,
                            null);
            silent =
                    simulatorExecutor.submit(
                            () -> {
                                new TelemetrySimulator().run(silentConfiguration);
                                return null;
                            });

            await().atMost(EVENTUAL)
                    .untilAsserted(
                            () ->
                                    assertEquals(
                                            SourceStatus.ONLINE,
                                            snapshot(runtime, "recovering-source").status()));
            assertFalse(silent.isDone(), "silent simulator ended before ONLINE was observed");

            await().atMost(EVENTUAL)
                    .untilAsserted(
                            () ->
                                    assertEquals(
                                            SourceStatus.STALE,
                                            snapshot(runtime, "recovering-source").status()));
            assertFalse(
                    silent.isDone(),
                    "silent simulator must still own its open TCP connection when STALE is observed");

            SourceSnapshot stale = snapshot(runtime, "recovering-source");
            new TelemetrySimulator()
                    .run(
                            simulator(
                                    runtime.port(),
                                    "recovering-source",
                                    SimulatorMode.DISCONNECT,
                                    Duration.ZERO,
                                    Duration.ZERO,
                                    1,
                                    null));

            await().atMost(EVENTUAL)
                    .untilAsserted(
                            () -> {
                                SourceSnapshot recovered =
                                        snapshot(runtime, "recovering-source");
                                assertEquals(SourceStatus.ONLINE, recovered.status());
                                assertEquals(
                                        stale.acceptedEventCount() + 1,
                                        recovered.acceptedEventCount());
                                assertEquals(
                                        stale.processedEventCount() + 1,
                                        recovered.processedEventCount());
                                assertFalse(
                                        recovered.lastAcceptedAt()
                                                .isBefore(stale.lastAcceptedAt()));
                            });
            assertFalse(silent.isDone(), "recovery must not depend on closing the silent connection");
        } finally {
            if (silent != null) {
                silent.cancel(true);
            }
            stop(simulatorExecutor);
        }
    }

    @Test
    @DisplayName("multiple simulator instances concurrently reach independent source state")
    void multipleSimulators() throws Exception {
        int simulatorCount = 6;
        int eventsPerSource = 3;
        ExecutorService simulators = Executors.newFixedThreadPool(simulatorCount);
        CountDownLatch start = new CountDownLatch(1);
        try (TelemetryMonitorTestRuntime runtime =
                new TelemetryMonitorTestRuntime(configuration(2, 16, LONG_STALE_AFTER))) {
            List<Future<?>> executions = new ArrayList<>();
            for (int index = 0; index < simulatorCount; index++) {
                String sourceId = "concurrent-source-" + index;
                SimulatorConfiguration configuration =
                        simulator(
                                runtime.port(),
                                sourceId,
                                SimulatorMode.DISCONNECT,
                                Duration.ZERO,
                                Duration.ZERO,
                                eventsPerSource,
                                null);
                executions.add(
                        simulators.submit(
                                () -> {
                                    if (!start.await(5, TimeUnit.SECONDS)) {
                                        throw new IllegalStateException("start gate was not released");
                                    }
                                    new TelemetrySimulator().run(configuration);
                                    return null;
                                }));
            }

            start.countDown();
            for (Future<?> execution : executions) {
                execution.get(10, TimeUnit.SECONDS);
            }

            await().atMost(EVENTUAL)
                    .untilAsserted(
                            () -> {
                                assertEquals(simulatorCount, runtime.registry().knownSourceCount());
                                for (int index = 0; index < simulatorCount; index++) {
                                    SourceSnapshot source =
                                            snapshot(runtime, "concurrent-source-" + index);
                                    assertEquals(eventsPerSource, source.acceptedEventCount());
                                    assertEquals(eventsPerSource, source.processedEventCount());
                                }
                            });
        } finally {
            start.countDown();
            stop(simulators);
        }
    }

    @Test
    @DisplayName("malformed input is isolated and the monitor serves subsequent valid traffic")
    void malformedInputIsIsolated() throws Exception {
        try (TelemetryMonitorTestRuntime runtime =
                new TelemetryMonitorTestRuntime(configuration(2, 16, LONG_STALE_AFTER))) {
            new TelemetrySimulator()
                    .run(
                            simulator(
                                    runtime.port(),
                                    "malformed-source",
                                    SimulatorMode.MALFORMED,
                                    Duration.ZERO,
                                    Duration.ZERO,
                                    0,
                                    MalformedCase.INVALID_VALUE));

            new TelemetrySimulator()
                    .run(
                            simulator(
                                    runtime.port(),
                                    "valid-after-malformed",
                                    SimulatorMode.DISCONNECT,
                                    Duration.ZERO,
                                    Duration.ZERO,
                                    1,
                                    null));

            await().atMost(EVENTUAL)
                    .untilAsserted(
                            () -> {
                                assertTrue(runtime.registry().snapshot("malformed-source").isEmpty());
                                SourceSnapshot valid = snapshot(runtime, "valid-after-malformed");
                                assertEquals(SourceStatus.ONLINE, valid.status());
                                assertEquals(1, valid.processedEventCount());
                            });
        }
    }

    @Test
    @DisplayName("disconnect leaves its source known and does not affect later clients")
    void disconnectIsIsolated() throws Exception {
        try (TelemetryMonitorTestRuntime runtime =
                new TelemetryMonitorTestRuntime(configuration(2, 16, LONG_STALE_AFTER))) {
            new TelemetrySimulator()
                    .run(
                            simulator(
                                    runtime.port(),
                                    "disconnect-source",
                                    SimulatorMode.DISCONNECT,
                                    Duration.ZERO,
                                    Duration.ZERO,
                                    4,
                                    null));

            await().atMost(EVENTUAL)
                    .untilAsserted(
                            () -> {
                                SourceSnapshot disconnected =
                                        snapshot(runtime, "disconnect-source");
                                assertEquals(4, disconnected.acceptedEventCount());
                                assertEquals(4, disconnected.processedEventCount());
                            });

            new TelemetrySimulator()
                    .run(
                            simulator(
                                    runtime.port(),
                                    "after-disconnect",
                                    SimulatorMode.DISCONNECT,
                                    Duration.ZERO,
                                    Duration.ZERO,
                                    1,
                                    null));

            await().atMost(EVENTUAL)
                    .untilAsserted(
                            () -> {
                                assertTrue(runtime.registry().snapshot("disconnect-source").isPresent());
                                assertEquals(
                                        1,
                                        snapshot(runtime, "after-disconnect")
                                                .processedEventCount());
                            });
        }
    }

    static MonitorConfiguration configuration(
            int workers, int queueCapacity, Duration staleAfter) throws Exception {
        return new MonitorConfiguration(
                InetAddress.getByName("127.0.0.1"),
                9100,
                16,
                workers,
                queueCapacity,
                8192,
                32,
                staleAfter,
                CHECK_INTERVAL,
                Duration.ofSeconds(5),
                LogLevel.INFO);
    }

    static SimulatorConfiguration simulator(
            int port,
            String sourceId,
            SimulatorMode mode,
            Duration interval,
            Duration duration,
            int count,
            MalformedCase malformedCase) {
        return new SimulatorConfiguration(
                "127.0.0.1",
                port,
                sourceId,
                mode,
                "temperature",
                20.0,
                interval,
                duration,
                count,
                malformedCase);
    }

    static SourceSnapshot snapshot(TelemetryMonitorTestRuntime runtime, String sourceId) {
        return runtime
                .registry()
                .snapshot(sourceId)
                .orElseThrow(() -> new AssertionError("no snapshot for " + sourceId));
    }

    static void stop(ExecutorService executor) throws InterruptedException {
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "test executor did not terminate");
    }
}
