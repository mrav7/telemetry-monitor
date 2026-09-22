package io.github.mrav7.telemetrymonitor.network;

import io.github.mrav7.telemetrymonitor.configuration.MonitorConfiguration;
import io.github.mrav7.telemetrymonitor.lifecycle.ShutdownCoordinator;
import io.github.mrav7.telemetrymonitor.processing.ProcessingPipeline;
import io.github.mrav7.telemetrymonitor.protocol.TelemetryEnvelope;
import io.github.mrav7.telemetrymonitor.state.SourceRegistry;
import io.github.mrav7.telemetrymonitor.state.StaleMonitor;
import java.io.IOException;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/** Same-JVM test composition of the real monitor components over an ephemeral TCP port. */
final class TelemetryMonitorTestRuntime implements AutoCloseable {

    private static final long TERMINATION_TIMEOUT_SECONDS = 10;

    private final SourceRegistry registry;
    private final ProcessingPipeline pipeline;
    private final StaleMonitor staleMonitor;
    private final TelemetryServer server;
    private final ShutdownCoordinator shutdown;
    private final ExecutorService acceptExecutor = Executors.newSingleThreadExecutor();
    private final Future<?> acceptLoop;

    TelemetryMonitorTestRuntime(MonitorConfiguration configuration) throws IOException {
        this(configuration, ignored -> {});
    }

    TelemetryMonitorTestRuntime(
            MonitorConfiguration configuration, Consumer<TelemetryEnvelope> beforeRegistryApply)
            throws IOException {
        Objects.requireNonNull(configuration, "configuration");
        Objects.requireNonNull(beforeRegistryApply, "beforeRegistryApply");

        Clock clock = Clock.systemUTC();
        registry = new SourceRegistry(configuration.maxSources());
        pipeline =
                new ProcessingPipeline(
                        configuration,
                        envelope -> {
                            beforeRegistryApply.accept(envelope);
                            registry.apply(envelope);
                        });
        staleMonitor = new StaleMonitor(configuration, clock, registry);
        server = new TelemetryServer(configuration, 0, clock, pipeline);
        server.bind();
        pipeline.start();
        staleMonitor.start();
        shutdown =
                new ShutdownCoordinator(
                        configuration.shutdownGrace(), server, pipeline, staleMonitor);
        acceptLoop = acceptExecutor.submit(server::serve);
    }

    int port() {
        return server.boundPort();
    }

    SourceRegistry registry() {
        return registry;
    }

    ProcessingPipeline pipeline() {
        return pipeline;
    }

    @Override
    public void close() throws Exception {
        shutdown.shutdown();
        Throwable failure = null;
        try {
            acceptLoop.get(TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            failure = e.getCause();
        } catch (TimeoutException e) {
            failure = e;
        } finally {
            acceptExecutor.shutdownNow();
            if (!acceptExecutor.awaitTermination(TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    && failure == null) {
                failure = new AssertionError("accept-loop executor did not terminate");
            }
        }
        if (failure != null) {
            if (failure instanceof Exception exception) {
                throw exception;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            throw new AssertionError("accept loop failed", failure);
        }
    }
}
