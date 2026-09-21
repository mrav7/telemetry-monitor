package io.github.mrav7.telemetrymonitor;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Guards the project baseline and the startup contract: valid configuration starts, invalid
 * configuration stops the process with a non-zero status and a diagnostic that names the problem.
 */
class TelemetryMonitorApplicationTest {

    private static final int EXPECTED_JAVA_FEATURE_RELEASE = 25;

    @Test
    @DisplayName("tests execute on the Java release the project targets")
    void runsOnExpectedJavaRelease() {
        assertEquals(
                EXPECTED_JAVA_FEATURE_RELEASE,
                Runtime.version().feature(),
                "tests must run on the Java release the project is compiled against");
    }

    @Nested
    @DisplayName("startup outcome")
    class StartupOutcome {

        @Test
        @DisplayName("an empty environment starts successfully on the frozen defaults")
        void defaultsStartSuccessfully() {
            assertEquals(
                    TelemetryMonitorApplication.EXIT_SUCCESS,
                    TelemetryMonitorApplication.run(Map.of()));
        }

        @Test
        @DisplayName("a fully specified valid environment starts successfully")
        void validOverridesStartSuccessfully() {
            assertEquals(
                    TelemetryMonitorApplication.EXIT_SUCCESS,
                    TelemetryMonitorApplication.run(
                            Map.of(
                                    "TM_BIND_ADDRESS", "0.0.0.0",
                                    "TM_PORT", "19100",
                                    "TM_LOG_LEVEL", "debug")));
        }

        @ParameterizedTest(name = "{0}={1} stops startup")
        @CsvSource({
            "TM_PORT, 0",
            "TM_PORT, 65536",
            "TM_PORT, not-a-number",
            "TM_BIND_ADDRESS, 999.1.1.1",
            "TM_WORKER_THREADS, 0",
            "TM_QUEUE_CAPACITY, -1",
            "TM_MAX_MESSAGE_BYTES, 0",
            "TM_LOG_LEVEL, OFF"
        })
        void invalidConfigurationStopsStartup(String variable, String value) {
            assertEquals(
                    TelemetryMonitorApplication.EXIT_INVALID_CONFIGURATION,
                    TelemetryMonitorApplication.run(Map.of(variable, value)));
        }

        @Test
        @DisplayName("the invalid-configuration code is non-zero")
        void invalidConfigurationCodeIsNonZero() {
            assertNotEquals(0, TelemetryMonitorApplication.EXIT_INVALID_CONFIGURATION);
        }
    }

    @Nested
    @DisplayName("process behaviour")
    class ProcessBehaviour {

        /** Launches the entry point in a separate JVM so the real exit status can be observed. */
        private ProcessResult launch(Map<String, String> telemetryEnvironment)
                throws IOException, InterruptedException {
            ProcessBuilder builder =
                    new ProcessBuilder(
                            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                            "-cp",
                            System.getProperty("java.class.path"),
                            TelemetryMonitorApplication.class.getName());

            // Start from a known state so a TM_* variable set in the developer's shell or on the
            // CI runner cannot change the outcome.
            builder.environment().keySet().removeIf(key -> key.startsWith("TM_"));
            builder.environment().putAll(telemetryEnvironment);
            builder.redirectErrorStream(false);

            Process process = builder.start();
            String stdout = new String(process.getInputStream().readAllBytes(), UTF_8);
            String stderr = new String(process.getErrorStream().readAllBytes(), UTF_8);
            assertTrue(
                    process.waitFor(60, TimeUnit.SECONDS), "startup process should not hang");
            return new ProcessResult(process.exitValue(), stdout, stderr);
        }

        @Test
        @DisplayName("valid configuration exits zero and logs the effective settings")
        void validConfigurationExitsZero() throws Exception {
            ProcessResult result = launch(Map.of());

            assertEquals(0, result.exitCode(), "stderr was: " + result.stderr());
            assertTrue(
                    result.stdout().contains("event=configuration"),
                    "startup should report its configuration, but stdout was: " + result.stdout());
            assertTrue(
                    result.stdout().contains("port=9100"),
                    "startup should report the default port, but stdout was: " + result.stdout());
        }

        @Test
        @DisplayName("invalid configuration exits non-zero with a diagnostic naming the variable")
        void invalidConfigurationExitsNonZero() throws Exception {
            ProcessResult result = launch(Map.of("TM_PORT", "70000"));

            assertNotEquals(0, result.exitCode(), "an unusable port must fail startup");
            assertTrue(
                    result.stderr().contains("TM_PORT"),
                    "the diagnostic should name the variable, but stderr was: " + result.stderr());
        }

        @Test
        @DisplayName("a rejected log level fails startup instead of being silently ignored")
        void rejectedLogLevelExitsNonZero() throws Exception {
            ProcessResult result = launch(Map.of("TM_LOG_LEVEL", "OFF"));

            assertNotEquals(0, result.exitCode());
            assertTrue(result.stderr().contains("TM_LOG_LEVEL"), result.stderr());
        }

        @Test
        @DisplayName("the configured log level takes effect in the running process")
        void configuredLogLevelIsApplied() throws Exception {
            ProcessResult atInfo = launch(Map.of("TM_LOG_LEVEL", "INFO"));
            ProcessResult atWarn = launch(Map.of("TM_LOG_LEVEL", "WARN"));

            assertEquals(0, atInfo.exitCode());
            assertEquals(0, atWarn.exitCode());
            assertTrue(
                    atInfo.stdout().contains("event=startup"),
                    "INFO should emit the startup record, but stdout was: " + atInfo.stdout());
            assertTrue(
                    atWarn.stdout().isBlank(),
                    "WARN should suppress the INFO records, but stdout was: " + atWarn.stdout());
        }

        private record ProcessResult(int exitCode, String stdout, String stderr) {}
    }
}
