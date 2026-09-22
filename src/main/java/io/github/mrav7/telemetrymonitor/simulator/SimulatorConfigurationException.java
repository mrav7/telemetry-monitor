package io.github.mrav7.telemetrymonitor.simulator;

/** Reports an unusable simulator command line before any network resource is opened. */
public final class SimulatorConfigurationException extends Exception {

    private static final long serialVersionUID = 1L;

    SimulatorConfigurationException(String message) {
        super(message);
    }
}
