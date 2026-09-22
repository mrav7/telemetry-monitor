package io.github.mrav7.telemetrymonitor.simulator;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Process entry point for the telemetry simulator. */
public final class TelemetrySimulatorApplication {

    public static final int EXIT_SUCCESS = 0;
    public static final int EXIT_INVALID_CONFIGURATION = 1;
    public static final int EXIT_EXECUTION_FAILURE = 2;

    @FunctionalInterface
    interface ScenarioRunner {
        void run(SimulatorConfiguration configuration) throws IOException, InterruptedException;
    }

    private static final Logger log = LoggerFactory.getLogger(TelemetrySimulatorApplication.class);

    private TelemetrySimulatorApplication() {}

    public static void main(String[] args) {
        System.exit(run(args));
    }

    public static int run(String[] args) {
        return run(args, new TelemetrySimulator()::run);
    }

    static int run(String[] args, ScenarioRunner runner) {
        SimulatorConfiguration configuration;
        try {
            configuration = SimulatorConfiguration.parse(args);
        } catch (SimulatorConfigurationException e) {
            System.err.println("Invalid simulator configuration: " + e.getMessage());
            return EXIT_INVALID_CONFIGURATION;
        }

        try {
            runner.run(configuration);
            return EXIT_SUCCESS;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error(
                    "event=simulator_failed mode={} source_id={} reason=interrupted",
                    configuration.mode().cliName(),
                    configuration.sourceId());
            return EXIT_EXECUTION_FAILURE;
        } catch (IOException | RuntimeException e) {
            log.error(
                    "event=simulator_failed mode={} source_id={} reason={}",
                    configuration.mode().cliName(),
                    configuration.sourceId(),
                    e.toString());
            return EXIT_EXECUTION_FAILURE;
        }
    }
}
