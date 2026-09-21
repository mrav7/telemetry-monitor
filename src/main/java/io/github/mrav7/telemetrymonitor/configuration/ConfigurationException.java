package io.github.mrav7.telemetrymonitor.configuration;

/**
 * Raised when an environment variable holds a value the monitor cannot operate on.
 *
 * <p>Carries the offending variable name separately from the message so that callers can react to
 * the variable without parsing the diagnostic text.
 */
public final class ConfigurationException extends Exception {

    private static final long serialVersionUID = 1L;

    private final String variable;

    public ConfigurationException(String variable, String detail) {
        super(variable + " " + detail);
        this.variable = variable;
    }

    public String variable() {
        return variable;
    }
}
