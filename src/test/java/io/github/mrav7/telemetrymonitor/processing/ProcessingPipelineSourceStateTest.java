package io.github.mrav7.telemetrymonitor.processing;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.mrav7.telemetrymonitor.configuration.ConfigurationException;
import io.github.mrav7.telemetrymonitor.configuration.ConfigurationLoader;
import io.github.mrav7.telemetrymonitor.configuration.MonitorConfiguration;
import io.github.mrav7.telemetrymonitor.protocol.ConnectionContext;
import io.github.mrav7.telemetrymonitor.protocol.TelemetryEnvelope;
import io.github.mrav7.telemetrymonitor.protocol.TelemetryEvent;
import io.github.mrav7.telemetrymonitor.state.SourceRegistry;
import io.github.mrav7.telemetrymonitor.state.SourceSnapshot;
import io.github.mrav7.telemetrymonitor.state.SourceStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Covers the join between the two halves of the processing side: the real bounded queue and its
 * real workers applying events to the real registry, wired exactly as the service wires them.
 *
 * <p>The registry's own invariants are covered directly in {@code SourceRegistryTest}. What is under
 * test here is that source state is reached from the pipeline at all, on worker threads, and that an
 * expected rejection there does not cost the service a worker.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class ProcessingPipelineSourceStateTest {

    private static final Instant T0 = Instant.parse("2026-09-21T18:15:42Z");
    private static final Duration BOUNDED_WAIT = Duration.ofSeconds(30);

    private ProcessingPipeline pipeline;

    @AfterEach
    void tearDown() {
        if (pipeline != null) {
            pipeline.close();
        }
    }

    private static MonitorConfiguration configuration() throws ConfigurationException {
        return ConfigurationLoader.load(
                Map.of(
                        ConfigurationLoader.QUEUE_CAPACITY, "16",
                        ConfigurationLoader.WORKER_THREADS, "4"));
    }

    private static TelemetryEnvelope envelope(String sourceId, Instant receivedAt) {
        return new TelemetryEnvelope(
                new TelemetryEvent(1, sourceId, receivedAt, "temperature", 18.72),
                receivedAt,
                new ConnectionContext(1, "/127.0.0.1:50000"));
    }

    private static SourceSnapshot snapshotOf(SourceRegistry registry, String sourceId) {
        return registry
                .snapshot(sourceId)
                .orElseThrow(() -> new AssertionError("no snapshot for " + sourceId));
    }

    @Test
    @DisplayName("events submitted to the pipeline reach source state through the workers")
    void submittedEventsReachSourceState() throws Exception {
        SourceRegistry registry = new SourceRegistry(10);
        pipeline = new ProcessingPipeline(configuration(), registry::apply);
        pipeline.start();

        int events = 50;
        for (int event = 1; event <= events; event++) {
            pipeline.submit(envelope("source-01", T0.plusMillis(event)));
            pipeline.submit(envelope("source-02", T0.plusMillis(event)));
        }

        await().atMost(BOUNDED_WAIT)
                .until(
                        () ->
                                registry.snapshot("source-01").isPresent()
                                        && snapshotOf(registry, "source-01").acceptedEventCount()
                                                == events
                                        && registry.snapshot("source-02").isPresent()
                                        && snapshotOf(registry, "source-02").acceptedEventCount()
                                                == events);

        SourceSnapshot first = snapshotOf(registry, "source-01");
        assertEquals(SourceStatus.ONLINE, first.status());
        // Four workers drain the queue, so which event admitted the source is theirs to decide.
        // What must hold is that the source ends as fresh as its newest event.
        assertTrue(
                !first.firstAcceptedAt().isBefore(T0.plusMillis(1))
                        && !first.firstAcceptedAt().isAfter(first.lastAcceptedAt()),
                "unexpected admission time: " + first.firstAcceptedAt());
        assertEquals(T0.plusMillis(events), first.lastAcceptedAt());
        assertEquals(events, first.processedEventCount());
        assertEquals(2, registry.knownSourceCount());
    }

    @Test
    @DisplayName("a source refused for capacity costs no worker and blocks no other source")
    void capacityRejectionDoesNotCostAWorker() throws Exception {
        SourceRegistry registry = new SourceRegistry(1);
        pipeline = new ProcessingPipeline(configuration(), registry::apply);
        pipeline.start();

        pipeline.submit(envelope("admitted", T0));
        await().atMost(BOUNDED_WAIT).until(() -> registry.snapshot("admitted").isPresent());

        // Enough refusals to have passed through every worker several times over.
        for (int refused = 0; refused < 40; refused++) {
            pipeline.submit(envelope("refused-" + refused, T0.plusMillis(refused)));
        }
        for (int event = 1; event <= 20; event++) {
            pipeline.submit(envelope("admitted", T0.plusSeconds(event)));
        }

        await().atMost(BOUNDED_WAIT)
                .until(() -> snapshotOf(registry, "admitted").acceptedEventCount() == 21);

        assertEquals(1, registry.knownSourceCount(), "no refused source may be created");
        assertEquals(T0.plusSeconds(20), snapshotOf(registry, "admitted").lastAcceptedAt());
        assertEquals(4, pipeline.workerCount());
        assertTrue(
                registry.snapshot("refused-0").isEmpty(),
                "a source refused for capacity must leave no state behind");
    }
}
