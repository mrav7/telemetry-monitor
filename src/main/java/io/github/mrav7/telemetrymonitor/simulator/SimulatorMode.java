package io.github.mrav7.telemetrymonitor.simulator;

import java.util.Locale;

/** Scenarios supported by the telemetry simulator. */
public enum SimulatorMode {
    NORMAL,
    BURST,
    MALFORMED,
    SILENT,
    DISCONNECT;

    static SimulatorMode parse(String value) throws SimulatorConfigurationException {
        try {
            return valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new SimulatorConfigurationException(
                    "--mode must be one of normal, burst, malformed, silent, disconnect, but was '"
                            + value
                            + "'");
        }
    }

    String cliName() {
        return name().toLowerCase(Locale.ROOT);
    }
}
