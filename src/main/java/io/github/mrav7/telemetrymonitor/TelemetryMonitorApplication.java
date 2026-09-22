package io.github.mrav7.telemetrymonitor;

import io.github.mrav7.telemetrymonitor.configuration.ConfigurationException;
import io.github.mrav7.telemetrymonitor.configuration.ConfigurationLoader;
import io.github.mrav7.telemetrymonitor.configuration.MonitorConfiguration;
import io.github.mrav7.telemetrymonitor.lifecycle.ShutdownCoordinator;
import io.github.mrav7.telemetrymonitor.network.TelemetryServer;
import io.github.mrav7.telemetrymonitor.processing.ProcessingPipeline;
import io.github.mrav7.telemetrymonitor.state.SourceRegistry;
import io.github.mrav7.telemetrymonitor.state.StaleMonitor;
import java.io.IOException;
import java.time.Clock;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point of the Telemetry Monitor service.
 *
 * <p>Startup validates configuration, opens the listener, starts the processing workers and the
 * scheduled staleness monitor, and then serves connections until the listener is closed. Accepted
 * telemetry is applied to the source registry by the processing workers.
 *
 * <p>A single coordinator owns the process lifecycle and the global shutdown deadline. The JVM
 * hook and the serving thread both use that same coordinator, so SIGTERM and ordinary cleanup join
 * one idempotent shutdown operation.
 */
public final class TelemetryMonitorApplication {

    /** Returned when configuration cannot be used; startup stops before anything is opened. */
    static final int EXIT_INVALID_CONFIGURATION = 1;

    /** Returned when the listener cannot be opened, for instance because the port is in use. */
    static final int EXIT_LISTENER_UNAVAILABLE = 2;

    static final int EXIT_SUCCESS = 0;

    private static final Logger log = LoggerFactory.getLogger(TelemetryMonitorApplication.class);

    private TelemetryMonitorApplication() {
        // Not instantiable: the class only exposes the process entry point.
    }

    public static void main(String[] args) {
        System.exit(run(System.getenv()));
    }

    /**
     * Runs the service against the supplied environment and returns the process exit code.
     *
     * <p>Separating this from {@link #main(String[])} keeps the exit decision testable without any
     * test needing to intercept {@code System.exit}. The call blocks while the service is serving.
     */
    static int run(Map<String, String> environment) {
        MonitorConfiguration configuration;
        try {
            configuration = ConfigurationLoader.load(environment);
        } catch (ConfigurationException e) {
            // Reported on stderr rather than through SLF4J: configuration is what decides the log
            // level, so the logger is not yet a trustworthy channel for this particular failure.
            System.err.println("Invalid configuration: " + e.getMessage());
            return EXIT_INVALID_CONFIGURATION;
        }

        log.info(
                "event=startup runtime_version={} vendor={}",
                Runtime.version(),
                System.getProperty("java.vendor"));
        log.info("event=configuration {}", configuration.describe());

        Clock clock = Clock.systemUTC();
        SourceRegistry registry = new SourceRegistry(configuration.maxSources());
        ProcessingPipeline pipeline = new ProcessingPipeline(configuration, registry::apply);
        StaleMonitor staleMonitor = new StaleMonitor(configuration, clock, registry);
        TelemetryServer server = new TelemetryServer(configuration, clock, pipeline);
        try {
            server.bind();
        } catch (IOException e) {
            log.error(
                    "event=server_start_failed bind_address={} port={} reason={}",
                    configuration.bindAddress().getHostAddress(),
                    configuration.port(),
                    e.toString());
            server.close();
            staleMonitor.close();
            pipeline.close();
            return EXIT_LISTENER_UNAVAILABLE;
        }

        ShutdownCoordinator shutdown =
                new ShutdownCoordinator(configuration.shutdownGrace(), server, pipeline, staleMonitor);
        Thread shutdownHook = new Thread(shutdown::shutdown, "tm-shutdown-hook");
        Runtime runtime = Runtime.getRuntime();
        boolean hookInstalled = false;
        try {
            pipeline.start();
            staleMonitor.start();
            runtime.addShutdownHook(shutdownHook);
            hookInstalled = true;
            server.serve();
        } finally {
            shutdown.shutdown();
            if (hookInstalled) {
                try {
                    runtime.removeShutdownHook(shutdownHook);
                } catch (IllegalStateException ignored) {
                    // The JVM is already shutting down and owns hook removal.
                }
            }
        }
        return EXIT_SUCCESS;
    }
}
