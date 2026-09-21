package io.github.mrav7.telemetrymonitor.configuration;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Covers the configuration contract: the frozen defaults, the accepted ranges, and the rule that a
 * value which is present but invalid fails startup instead of falling back to its default.
 */
class ConfigurationLoaderTest {

    private static Map<String, String> with(String variable, String value) {
        Map<String, String> environment = new HashMap<>();
        environment.put(variable, value);
        return environment;
    }

    private static ConfigurationException rejects(String variable, String value) {
        return assertThrows(
                ConfigurationException.class, () -> ConfigurationLoader.load(with(variable, value)));
    }

    @Nested
    @DisplayName("defaults")
    class Defaults {

        @Test
        @DisplayName("an empty environment yields exactly the frozen defaults")
        void emptyEnvironmentYieldsFrozenDefaults() throws Exception {
            MonitorConfiguration configuration = ConfigurationLoader.load(Map.of());

            assertAll(
                    () -> assertEquals("127.0.0.1", configuration.bindAddress().getHostAddress()),
                    () -> assertEquals(9100, configuration.port()),
                    () -> assertEquals(256, configuration.maxConnections()),
                    () -> assertEquals(4, configuration.workerThreads()),
                    () -> assertEquals(1024, configuration.queueCapacity()),
                    () -> assertEquals(8192, configuration.maxMessageBytes()),
                    () -> assertEquals(10_000, configuration.maxSources()),
                    () -> assertEquals(Duration.ofSeconds(30), configuration.staleAfter()),
                    () -> assertEquals(Duration.ofSeconds(5), configuration.staleCheckInterval()),
                    () -> assertEquals(Duration.ofSeconds(10), configuration.shutdownGrace()),
                    () -> assertEquals(LogLevel.INFO, configuration.logLevel()));
        }

        @Test
        @DisplayName("overriding one variable leaves the others at their defaults")
        void partialOverrideKeepsRemainingDefaults() throws Exception {
            MonitorConfiguration configuration =
                    ConfigurationLoader.load(with(ConfigurationLoader.PORT, "9999"));

            assertAll(
                    () -> assertEquals(9999, configuration.port()),
                    () -> assertEquals(256, configuration.maxConnections()),
                    () -> assertEquals(LogLevel.INFO, configuration.logLevel()));
        }
    }

    @Test
    @DisplayName("every variable can be overridden at once")
    void completeOverride() throws Exception {
        MonitorConfiguration configuration =
                ConfigurationLoader.load(
                        Map.ofEntries(
                                Map.entry(ConfigurationLoader.BIND_ADDRESS, "0.0.0.0"),
                                Map.entry(ConfigurationLoader.PORT, "1234"),
                                Map.entry(ConfigurationLoader.MAX_CONNECTIONS, "12"),
                                Map.entry(ConfigurationLoader.WORKER_THREADS, "3"),
                                Map.entry(ConfigurationLoader.QUEUE_CAPACITY, "7"),
                                Map.entry(ConfigurationLoader.MAX_MESSAGE_BYTES, "64"),
                                Map.entry(ConfigurationLoader.MAX_SOURCES, "5"),
                                Map.entry(ConfigurationLoader.STALE_AFTER_SECONDS, "2"),
                                Map.entry(ConfigurationLoader.STALE_CHECK_INTERVAL_SECONDS, "1"),
                                Map.entry(ConfigurationLoader.SHUTDOWN_GRACE_SECONDS, "6"),
                                Map.entry(ConfigurationLoader.LOG_LEVEL, "DEBUG")));

        assertAll(
                () -> assertEquals("0.0.0.0", configuration.bindAddress().getHostAddress()),
                () -> assertEquals(1234, configuration.port()),
                () -> assertEquals(12, configuration.maxConnections()),
                () -> assertEquals(3, configuration.workerThreads()),
                () -> assertEquals(7, configuration.queueCapacity()),
                () -> assertEquals(64, configuration.maxMessageBytes()),
                () -> assertEquals(5, configuration.maxSources()),
                () -> assertEquals(Duration.ofSeconds(2), configuration.staleAfter()),
                () -> assertEquals(Duration.ofSeconds(1), configuration.staleCheckInterval()),
                () -> assertEquals(Duration.ofSeconds(6), configuration.shutdownGrace()),
                () -> assertEquals(LogLevel.DEBUG, configuration.logLevel()));
    }

    @Nested
    @DisplayName("port")
    class Port {

        @ParameterizedTest(name = "port {0} is accepted")
        @ValueSource(strings = {"1", "9100", "65535"})
        void acceptsInRangePorts(String port) throws Exception {
            assertEquals(
                    Integer.parseInt(port),
                    ConfigurationLoader.load(with(ConfigurationLoader.PORT, port)).port());
        }

        @ParameterizedTest(name = "port {0} is rejected")
        @ValueSource(strings = {"0", "-1", "65536", "99999"})
        void rejectsOutOfRangePorts(String port) {
            assertEquals(
                    ConfigurationLoader.PORT, rejects(ConfigurationLoader.PORT, port).variable());
        }

        @Test
        @DisplayName("port 0 is rejected even though ephemeral ports are useful in tests")
        void rejectsEphemeralPortZero() {
            assertEquals(
                    ConfigurationLoader.PORT, rejects(ConfigurationLoader.PORT, "0").variable());
        }

        @ParameterizedTest(name = "malformed port {0} is rejected")
        @ValueSource(strings = {"abc", "12.5", "9100x", " 9100", "0x1f"})
        void rejectsMalformedPorts(String port) {
            rejects(ConfigurationLoader.PORT, port);
        }

        @Test
        @DisplayName("a numeric value beyond int range is rejected rather than wrapping")
        void rejectsNumericOverflow() {
            rejects(ConfigurationLoader.PORT, "2147483648");
            rejects(ConfigurationLoader.MAX_SOURCES, "9999999999999999999");
        }
    }

    @Nested
    @DisplayName("positive integers")
    class PositiveIntegers {

        @ParameterizedTest(name = "{0} rejects {1}")
        @CsvSource({
            "TM_MAX_CONNECTIONS, 0",
            "TM_MAX_CONNECTIONS, -1",
            "TM_WORKER_THREADS, 0",
            "TM_WORKER_THREADS, -4",
            "TM_QUEUE_CAPACITY, 0",
            "TM_QUEUE_CAPACITY, -1024",
            "TM_MAX_MESSAGE_BYTES, 0",
            "TM_MAX_MESSAGE_BYTES, -8192",
            "TM_MAX_SOURCES, 0",
            "TM_MAX_SOURCES, -1",
            "TM_STALE_AFTER_SECONDS, 0",
            "TM_STALE_AFTER_SECONDS, -30",
            "TM_STALE_CHECK_INTERVAL_SECONDS, 0",
            "TM_STALE_CHECK_INTERVAL_SECONDS, -5",
            "TM_SHUTDOWN_GRACE_SECONDS, 0",
            "TM_SHUTDOWN_GRACE_SECONDS, -10"
        })
        void rejectsNonPositiveValues(String variable, String value) {
            assertEquals(variable, rejects(variable, value).variable());
        }

        @ParameterizedTest(name = "{0} rejects a malformed number")
        @ValueSource(
                strings = {
                    "TM_MAX_CONNECTIONS",
                    "TM_WORKER_THREADS",
                    "TM_QUEUE_CAPACITY",
                    "TM_MAX_MESSAGE_BYTES",
                    "TM_MAX_SOURCES",
                    "TM_STALE_AFTER_SECONDS",
                    "TM_STALE_CHECK_INTERVAL_SECONDS",
                    "TM_SHUTDOWN_GRACE_SECONDS"
                })
        void rejectsMalformedNumbers(String variable) {
            assertEquals(variable, rejects(variable, "not-a-number").variable());
        }
    }

    @Nested
    @DisplayName("bind address")
    class BindAddress {

        @ParameterizedTest(name = "{0} is accepted")
        @ValueSource(strings = {"127.0.0.1", "0.0.0.0", "192.168.1.10", "::1", "::"})
        void acceptsLiteralAddresses(String address) throws Exception {
            ConfigurationLoader.load(with(ConfigurationLoader.BIND_ADDRESS, address));
        }

        @ParameterizedTest(name = "{0} is rejected")
        @ValueSource(
                strings = {"999.1.1.1", "not-an-address", "127.0.0.1:9100", "1.2.3.4.5", "0x7f.0.0.1"})
        void rejectsInvalidAddresses(String address) {
            assertEquals(
                    ConfigurationLoader.BIND_ADDRESS,
                    rejects(ConfigurationLoader.BIND_ADDRESS, address).variable());
        }

        @ParameterizedTest(name = "the shorthand form {0} expands to {1}")
        @CsvSource({"1.2.3, 1.2.0.3", "127.1, 127.0.0.1", "1.2, 1.0.0.2"})
        @DisplayName("abbreviated IPv4 literals are accepted and expanded by the JDK")
        void acceptsAbbreviatedIpv4Literals(String configured, String expected) throws Exception {
            // These are long-standing IPv4 textual forms that java.net accepts. Documented here so
            // the behaviour is a deliberate consequence of using JDK address parsing rather than a
            // surprise discovered in production.
            assertEquals(
                    expected,
                    ConfigurationLoader.load(with(ConfigurationLoader.BIND_ADDRESS, configured))
                            .bindAddress()
                            .getHostAddress());
        }

        @Test
        @DisplayName("a hostname is rejected, so validation never depends on DNS")
        void rejectsHostnamesToStayDnsIndependent() {
            rejects(ConfigurationLoader.BIND_ADDRESS, "localhost");
            rejects(ConfigurationLoader.BIND_ADDRESS, "example.com");
        }
    }

    @Nested
    @DisplayName("log level")
    class Level {

        @ParameterizedTest(name = "{0} maps to {1}")
        @CsvSource({
            "TRACE, TRACE",
            "DEBUG, DEBUG",
            "INFO, INFO",
            "WARN, WARN",
            "ERROR, ERROR",
            "trace, TRACE",
            "Info, INFO",
            "wArN, WARN"
        })
        void acceptsKnownLevelsIgnoringCase(String configured, LogLevel expected) throws Exception {
            assertEquals(
                    expected,
                    ConfigurationLoader.load(with(ConfigurationLoader.LOG_LEVEL, configured))
                            .logLevel());
        }

        @ParameterizedTest(name = "{0} is rejected")
        @ValueSource(strings = {"ALL", "OFF", "VERBOSE", "info ", "1"})
        void rejectsUnsupportedLevels(String configured) {
            assertEquals(
                    ConfigurationLoader.LOG_LEVEL,
                    rejects(ConfigurationLoader.LOG_LEVEL, configured).variable());
        }
    }

    @ParameterizedTest(name = "a blank {0} is rejected rather than treated as absent")
    @ValueSource(
            strings = {
                "TM_BIND_ADDRESS",
                "TM_PORT",
                "TM_MAX_CONNECTIONS",
                "TM_WORKER_THREADS",
                "TM_QUEUE_CAPACITY",
                "TM_MAX_MESSAGE_BYTES",
                "TM_MAX_SOURCES",
                "TM_STALE_AFTER_SECONDS",
                "TM_STALE_CHECK_INTERVAL_SECONDS",
                "TM_SHUTDOWN_GRACE_SECONDS",
                "TM_LOG_LEVEL"
            })
    void rejectsBlankValues(String variable) {
        assertEquals(variable, rejects(variable, "   ").variable());
        assertEquals(variable, rejects(variable, "").variable());
    }

    @Test
    @DisplayName("the diagnostic names the offending variable")
    void diagnosticNamesTheVariable() {
        ConfigurationException failure = rejects(ConfigurationLoader.PORT, "70000");
        org.junit.jupiter.api.Assertions.assertTrue(
                failure.getMessage().contains(ConfigurationLoader.PORT),
                "message should name the variable: " + failure.getMessage());
    }
}
