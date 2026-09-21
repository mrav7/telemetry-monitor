package io.github.mrav7.telemetrymonitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point of the Telemetry Monitor service.
 *
 * <p>At this stage the application only establishes the runtime baseline: it starts, reports the
 * JVM it is running on, and exits normally. Configuration, the TCP listener and the processing
 * pipeline are introduced by later work.
 */
public final class TelemetryMonitorApplication {

    private static final Logger log = LoggerFactory.getLogger(TelemetryMonitorApplication.class);

    private TelemetryMonitorApplication() {
        // Not instantiable: the class only exposes the process entry point.
    }

    public static void main(String[] args) {
        log.info("event=startup runtime_version={} vendor={}",
                Runtime.version(),
                System.getProperty("java.vendor"));
    }
}
