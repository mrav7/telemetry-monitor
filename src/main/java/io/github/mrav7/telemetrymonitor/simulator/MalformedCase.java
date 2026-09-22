package io.github.mrav7.telemetrymonitor.simulator;

import java.util.Arrays;

/** Known application-message faults emitted by malformed mode. */
public enum MalformedCase {
    BROKEN_JSON("broken-json"),
    MISSING_FIELD("missing-field"),
    UNSUPPORTED_VERSION("unsupported-version"),
    INVALID_SOURCE_ID("invalid-source-id"),
    INVALID_TIMESTAMP("invalid-timestamp"),
    INVALID_VALUE("invalid-value");

    private final String cliName;

    MalformedCase(String cliName) {
        this.cliName = cliName;
    }

    static MalformedCase parse(String value) throws SimulatorConfigurationException {
        return Arrays.stream(values())
                .filter(candidate -> candidate.cliName.equals(value))
                .findFirst()
                .orElseThrow(
                        () ->
                                new SimulatorConfigurationException(
                                        "--case must be one of broken-json, missing-field, "
                                                + "unsupported-version, invalid-source-id, "
                                                + "invalid-timestamp, invalid-value, but was '"
                                                + value
                                                + "'"));
    }

    public String cliName() {
        return cliName;
    }
}
