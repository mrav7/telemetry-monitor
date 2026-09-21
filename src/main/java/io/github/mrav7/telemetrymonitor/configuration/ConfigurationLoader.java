package io.github.mrav7.telemetrymonitor.configuration;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Map;

/**
 * Builds a {@link MonitorConfiguration} from environment variables.
 *
 * <p>The loader reads from a supplied map rather than calling {@code System.getenv()} directly, so
 * tests can drive every branch without mutating the real process environment.
 *
 * <p>Validation is strict on purpose. A variable that is absent falls back to its default, but a
 * variable that is present and invalid fails startup: values are never clamped, trimmed,
 * normalised or replaced by the default.
 */
public final class ConfigurationLoader {

    public static final String BIND_ADDRESS = "TM_BIND_ADDRESS";
    public static final String PORT = "TM_PORT";
    public static final String MAX_CONNECTIONS = "TM_MAX_CONNECTIONS";
    public static final String WORKER_THREADS = "TM_WORKER_THREADS";
    public static final String QUEUE_CAPACITY = "TM_QUEUE_CAPACITY";
    public static final String MAX_MESSAGE_BYTES = "TM_MAX_MESSAGE_BYTES";
    public static final String MAX_SOURCES = "TM_MAX_SOURCES";
    public static final String STALE_AFTER_SECONDS = "TM_STALE_AFTER_SECONDS";
    public static final String STALE_CHECK_INTERVAL_SECONDS = "TM_STALE_CHECK_INTERVAL_SECONDS";
    public static final String SHUTDOWN_GRACE_SECONDS = "TM_SHUTDOWN_GRACE_SECONDS";
    public static final String LOG_LEVEL = "TM_LOG_LEVEL";

    public static final String DEFAULT_BIND_ADDRESS = "127.0.0.1";
    public static final int DEFAULT_PORT = 9100;
    public static final int DEFAULT_MAX_CONNECTIONS = 256;
    public static final int DEFAULT_WORKER_THREADS = 4;
    public static final int DEFAULT_QUEUE_CAPACITY = 1024;
    public static final int DEFAULT_MAX_MESSAGE_BYTES = 8192;
    public static final int DEFAULT_MAX_SOURCES = 10_000;
    public static final int DEFAULT_STALE_AFTER_SECONDS = 30;
    public static final int DEFAULT_STALE_CHECK_INTERVAL_SECONDS = 5;
    public static final int DEFAULT_SHUTDOWN_GRACE_SECONDS = 10;
    public static final LogLevel DEFAULT_LOG_LEVEL = LogLevel.INFO;

    private static final int MIN_PORT = 1;
    private static final int MAX_PORT = 65_535;

    private ConfigurationLoader() {
        // Static entry points only.
    }

    /** Loads configuration from the real process environment. */
    public static MonitorConfiguration loadFromEnvironment() throws ConfigurationException {
        return load(System.getenv());
    }

    /** Loads configuration from the supplied variables. */
    public static MonitorConfiguration load(Map<String, String> environment)
            throws ConfigurationException {
        return new MonitorConfiguration(
                bindAddress(environment),
                bounded(environment, PORT, DEFAULT_PORT, MIN_PORT, MAX_PORT),
                positive(environment, MAX_CONNECTIONS, DEFAULT_MAX_CONNECTIONS),
                positive(environment, WORKER_THREADS, DEFAULT_WORKER_THREADS),
                positive(environment, QUEUE_CAPACITY, DEFAULT_QUEUE_CAPACITY),
                positive(environment, MAX_MESSAGE_BYTES, DEFAULT_MAX_MESSAGE_BYTES),
                positive(environment, MAX_SOURCES, DEFAULT_MAX_SOURCES),
                positiveSeconds(environment, STALE_AFTER_SECONDS, DEFAULT_STALE_AFTER_SECONDS),
                positiveSeconds(
                        environment,
                        STALE_CHECK_INTERVAL_SECONDS,
                        DEFAULT_STALE_CHECK_INTERVAL_SECONDS),
                positiveSeconds(environment, SHUTDOWN_GRACE_SECONDS, DEFAULT_SHUTDOWN_GRACE_SECONDS),
                logLevel(environment));
    }

    private static InetAddress bindAddress(Map<String, String> environment)
            throws ConfigurationException {
        String raw = present(environment, BIND_ADDRESS);
        String value = raw == null ? DEFAULT_BIND_ADDRESS : raw;
        try {
            // Literal IPv4/IPv6 only: parsing never consults DNS, so startup and the test suite
            // do not depend on name resolution.
            return InetAddress.ofLiteral(value);
        } catch (IllegalArgumentException e) {
            throw new ConfigurationException(
                    BIND_ADDRESS, "must be a literal IPv4 or IPv6 address, but was '" + value + "'");
        }
    }

    private static LogLevel logLevel(Map<String, String> environment) throws ConfigurationException {
        String raw = present(environment, LOG_LEVEL);
        if (raw == null) {
            return DEFAULT_LOG_LEVEL;
        }
        try {
            return LogLevel.parse(raw);
        } catch (IllegalArgumentException e) {
            throw new ConfigurationException(
                    LOG_LEVEL,
                    "must be one of TRACE, DEBUG, INFO, WARN, ERROR, but was '" + raw + "'");
        }
    }

    private static Duration positiveSeconds(
            Map<String, String> environment, String variable, int defaultSeconds)
            throws ConfigurationException {
        return Duration.ofSeconds(positive(environment, variable, defaultSeconds));
    }

    private static int positive(Map<String, String> environment, String variable, int defaultValue)
            throws ConfigurationException {
        return bounded(environment, variable, defaultValue, 1, Integer.MAX_VALUE);
    }

    private static int bounded(
            Map<String, String> environment, String variable, int defaultValue, int min, int max)
            throws ConfigurationException {
        String raw = present(environment, variable);
        if (raw == null) {
            return defaultValue;
        }
        int value;
        try {
            value = Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            // Also covers values that overflow an int.
            throw new ConfigurationException(
                    variable, "must be an integer, but was '" + raw + "'");
        }
        if (value < min || value > max) {
            throw new ConfigurationException(
                    variable, "must be between " + min + " and " + max + ", but was " + value);
        }
        return value;
    }

    /**
     * Returns the configured value, or {@code null} when the variable is absent.
     *
     * <p>A variable that is present but blank is a configuration error rather than a request for
     * the default.
     */
    private static String present(Map<String, String> environment, String variable)
            throws ConfigurationException {
        String raw = environment.get(variable);
        if (raw == null) {
            return null;
        }
        if (raw.isBlank()) {
            throw new ConfigurationException(variable, "must not be blank");
        }
        return raw;
    }
}
