package io.github.mrav7.telemetrymonitor;

import io.github.mrav7.telemetrymonitor.configuration.ConfigurationException;
import io.github.mrav7.telemetrymonitor.configuration.ConfigurationLoader;
import io.github.mrav7.telemetrymonitor.configuration.MonitorConfiguration;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point of the Telemetry Monitor service.
 *
 * <p>At this stage startup loads and validates configuration, reports it, and returns. The TCP
 * listener and the processing pipeline are introduced by later work.
 */
public final class TelemetryMonitorApplication {

    /** Returned when configuration cannot be used; startup stops before anything is opened. */
    static final int EXIT_INVALID_CONFIGURATION = 1;

    static final int EXIT_SUCCESS = 0;

    private TelemetryMonitorApplication() {
        // Not instantiable: the class only exposes the process entry point.
    }

    public static void main(String[] args) {
        System.exit(run(System.getenv()));
    }

    /**
     * Runs startup against the supplied environment and returns the process exit code.
     *
     * <p>Separating this from {@link #main(String[])} keeps the exit decision testable without any
     * test needing to intercept {@code System.exit}.
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

        Logger log = LoggerFactory.getLogger(TelemetryMonitorApplication.class);
        log.info(
                "event=startup runtime_version={} vendor={}",
                Runtime.version(),
                System.getProperty("java.vendor"));
        log.info("event=configuration {}", configuration.describe());
        return EXIT_SUCCESS;
    }
}
