package io.github.mrav7.telemetrymonitor;

import io.github.mrav7.telemetrymonitor.configuration.ConfigurationException;
import io.github.mrav7.telemetrymonitor.configuration.ConfigurationLoader;
import io.github.mrav7.telemetrymonitor.configuration.MonitorConfiguration;
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
 * <p>The runtime owns the pieces it wires together and releases them in reverse order. That is
 * resource termination, not a shutdown policy: the service-wide deadline, the producer stop and the
 * final queue drain are later work.
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
        try (StaleMonitor staleMonitor = new StaleMonitor(configuration, clock, registry);
                TelemetryServer server = new TelemetryServer(configuration, clock, pipeline)) {
            try {
                server.bind();
            } catch (IOException e) {
                log.error(
                        "event=server_start_failed bind_address={} port={} reason={}",
                        configuration.bindAddress().getHostAddress(),
                        configuration.port(),
                        e.toString());
                return EXIT_LISTENER_UNAVAILABLE;
            }
            pipeline.start();
            staleMonitor.start();
            server.serve();
        } finally {
            pipeline.close();
        }
        return EXIT_SUCCESS;
    }
}
