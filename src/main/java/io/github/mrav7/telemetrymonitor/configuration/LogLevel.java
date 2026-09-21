package io.github.mrav7.telemetrymonitor.configuration;

import java.util.Locale;

/**
 * Log levels the monitor accepts in {@code TM_LOG_LEVEL}.
 *
 * <p>Deliberately narrower than the set Logback understands: {@code ALL} and {@code OFF} are not
 * operational choices for this service and are rejected rather than silently honoured.
 */
public enum LogLevel {
    TRACE,
    DEBUG,
    INFO,
    WARN,
    ERROR;

    /**
     * Parses a configured level, ignoring case.
     *
     * @throws IllegalArgumentException if the value is not one of the accepted levels
     */
    public static LogLevel parse(String value) {
        return valueOf(value.toUpperCase(Locale.ROOT));
    }
}
