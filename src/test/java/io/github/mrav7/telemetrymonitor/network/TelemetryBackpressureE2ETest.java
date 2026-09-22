package io.github.mrav7.telemetrymonitor.network;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.mrav7.telemetrymonitor.protocol.TelemetryEnvelope;
import io.github.mrav7.telemetrymonitor.simulator.SimulatorConfiguration;
import io.github.mrav7.telemetrymonitor.simulator.SimulatorMode;
import io.github.mrav7.telemetrymonitor.simulator.TelemetrySimulator;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 20, unit = TimeUnit.SECONDS)
class TelemetryBackpressureE2ETest {

    private static final Duration EVENTUAL = Duration.ofSeconds(10);

    @Test
    @DisplayName("burst traffic blocks a producer behind one in-flight and one queued event")
    void burstExercisesBoundedBackpressure() throws Exception {
        FirstWorkerGate gate = new FirstWorkerGate();
        ExecutorService simulatorExecutor = Executors.newSingleThreadExecutor();
        try (TelemetryMonitorTestRuntime runtime =
                new TelemetryMonitorTestRuntime(
                        TelemetrySimulatorE2ETest.configuration(
                                1, 1, Duration.ofSeconds(30)),
                        gate::beforeRegistryApply)) {
            SimulatorConfiguration burst =
                    TelemetrySimulatorE2ETest.simulator(
                            runtime.port(),
                            "burst-source",
                            SimulatorMode.BURST,
                            Duration.ZERO,
                            Duration.ZERO,
                            3,
                            null);
            Future<?> execution =
                    simulatorExecutor.submit(
                            () -> {
                                new TelemetrySimulator().run(burst);
                                return null;
                            });

            try {
                assertTrue(gate.awaitEntered(), "the first event never entered processing");
                await().atMost(EVENTUAL)
                        .untilAsserted(
                                () -> {
                                    assertEquals(1, runtime.pipeline().inFlightCount());
                                    assertEquals(1, runtime.pipeline().queueDepth());
                                    assertTrue(
                                            runtime.pipeline().outstandingWorkCount() >= 3,
                                            "a third submission must be reserved beyond queue capacity");
                                });
            } finally {
                gate.release();
            }

            execution.get(10, TimeUnit.SECONDS);
            await().atMost(EVENTUAL)
                    .untilAsserted(
                            () -> {
                                assertEquals(
                                        3,
                                        TelemetrySimulatorE2ETest.snapshot(runtime, "burst-source")
                                                .processedEventCount());
                                assertEquals(
                                        3,
                                        TelemetrySimulatorE2ETest.snapshot(runtime, "burst-source")
                                                .acceptedEventCount());
                                assertEquals(0, runtime.pipeline().queueDepth());
                                assertEquals(0, runtime.pipeline().outstandingWorkCount());
                            });
        } finally {
            gate.release();
            TelemetrySimulatorE2ETest.stop(simulatorExecutor);
        }
    }

    private static final class FirstWorkerGate {

        private final AtomicBoolean first = new AtomicBoolean(true);
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        void beforeRegistryApply(TelemetryEnvelope ignored) {
            if (!first.compareAndSet(true, false)) {
                return;
            }
            entered.countDown();
            try {
                if (!release.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("first-worker gate was not released");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("first-worker gate was interrupted", e);
            }
        }

        boolean awaitEntered() throws InterruptedException {
            return entered.await(5, TimeUnit.SECONDS);
        }

        void release() {
            release.countDown();
        }
    }
}
