package io.github.mrav7.telemetrymonitor;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Guards the project baseline and the startup contract: a service that cannot be configured, or
 * whose listener cannot be opened, stops with a non-zero status and a diagnostic naming the
 * problem, and a service that starts really does listen.
 *
 * <p>Startup now blocks while serving, so the in-process tests here cover only the paths that
 * return before the listener is opened, and the process tests observe a running service and then
 * end it. What the running service does with telemetry is covered over real sockets by
 * {@code TelemetryServerTest}.
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
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
        @DisplayName("an occupied port stops startup instead of appearing to serve")
        void occupiedPortStopsStartup() throws IOException {
            // The port is held for the duration of the test, so the bind can only fail. Taking it
            // from the OS keeps the suite off any fixed port without TM_PORT having to accept 0.
            try (ServerSocket occupied = new ServerSocket()) {
                occupied.bind(new InetSocketAddress("127.0.0.1", 0));

                assertEquals(
                        TelemetryMonitorApplication.EXIT_LISTENER_UNAVAILABLE,
                        TelemetryMonitorApplication.run(
                                Map.of(
                                        "TM_BIND_ADDRESS", "127.0.0.1",
                                        "TM_PORT", String.valueOf(occupied.getLocalPort()))));
            }
        }

        @Test
        @DisplayName("the listener-unavailable code is non-zero and distinct")
        void listenerUnavailableCodeIsNonZero() {
            assertNotEquals(0, TelemetryMonitorApplication.EXIT_LISTENER_UNAVAILABLE);
            assertNotEquals(
                    TelemetryMonitorApplication.EXIT_INVALID_CONFIGURATION,
                    TelemetryMonitorApplication.EXIT_LISTENER_UNAVAILABLE);
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

        private static final long BOUNDED_WAIT_SECONDS = 30;

        private ProcessBuilder monitorProcess(Map<String, String> telemetryEnvironment) {
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
            return builder;
        }

        /** Launches a configuration that must fail, so the process ends by itself. */
        private ProcessResult launchUntilExit(Map<String, String> telemetryEnvironment)
                throws IOException, InterruptedException {
            Process process = monitorProcess(telemetryEnvironment).start();
            String stdout = new String(process.getInputStream().readAllBytes(), UTF_8);
            String stderr = new String(process.getErrorStream().readAllBytes(), UTF_8);
            assertTrue(
                    process.waitFor(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS),
                    "a failing startup should not keep the process alive");
            return new ProcessResult(process.exitValue(), stdout, stderr);
        }

        /**
         * A port the monitor process can be told to use.
         *
         * <p>The child cannot report a port back before it binds, so one is taken from the OS and
         * released immediately. This keeps the suite off any fixed port; the in-JVM tests use the
         * server's port-0 seam instead and have no such window.
         */
        private int reservePort() throws IOException {
            try (ServerSocket reserved = new ServerSocket()) {
                reserved.bind(new InetSocketAddress("127.0.0.1", 0));
                return reserved.getLocalPort();
            }
        }

        /** A monitor process running until the test ends it. */
        private final class RunningMonitor implements AutoCloseable {

            private final Process process;
            private final StringBuilder stdout = new StringBuilder();
            private final Thread outPump;
            private final Thread errPump;

            private RunningMonitor(Map<String, String> telemetryEnvironment) throws IOException {
                this.process = monitorProcess(telemetryEnvironment).start();
                this.outPump = pump(process.getInputStream(), stdout);
                this.errPump = pump(process.getErrorStream(), new StringBuilder());
            }

            private Thread pump(java.io.InputStream stream, StringBuilder sink) {
                return Thread.ofPlatform()
                        .daemon()
                        .start(
                                () -> {
                                    try (java.io.BufferedReader reader =
                                            new java.io.BufferedReader(
                                                    new java.io.InputStreamReader(stream, UTF_8))) {
                                        String line;
                                        while ((line = reader.readLine()) != null) {
                                            synchronized (sink) {
                                                sink.append(line).append('\n');
                                            }
                                        }
                                    } catch (IOException e) {
                                        // The stream ends when the process does.
                                    }
                                });
            }

            String stdout() {
                synchronized (stdout) {
                    return stdout.toString();
                }
            }

            long pid() {
                return process.pid();
            }

            /** Waits until the monitor answers on its port, which is its readiness signal. */
            void awaitListening(int port) throws Exception {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(BOUNDED_WAIT_SECONDS);
                while (System.nanoTime() < deadline) {
                    assertTrue(process.isAlive(), "the monitor exited instead of listening");
                    try (Socket probe = new Socket()) {
                        probe.connect(new InetSocketAddress("127.0.0.1", port), 500);
                        return;
                    } catch (IOException notYet) {
                        Thread.onSpinWait();
                    }
                }
                throw new AssertionError("the monitor never started listening on port " + port);
            }

            /**
             * Waits for a line to appear on the monitor's stdout.
             *
             * <p>Answering on the port and having its log line read by this test are two different
             * events, so a test that asserts on output waits for the output itself.
             */
            void awaitStdout(String marker) {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(BOUNDED_WAIT_SECONDS);
                while (System.nanoTime() < deadline) {
                    if (stdout().contains(marker)) {
                        return;
                    }
                    Thread.onSpinWait();
                }
                throw new AssertionError(
                        "the monitor never logged '" + marker + "'; stdout was: " + stdout());
            }

            void terminate() throws Exception {
                Process signal =
                        new ProcessBuilder("kill", "-TERM", Long.toString(process.pid())).start();
                assertTrue(signal.waitFor(5, TimeUnit.SECONDS), "kill -TERM should return");
                assertEquals(0, signal.exitValue(), "kill -TERM should signal the monitor");
                assertTrue(
                        process.waitFor(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS),
                        "the monitor process should end after SIGTERM");
                outPump.join(TimeUnit.SECONDS.toMillis(5));
                errPump.join(TimeUnit.SECONDS.toMillis(5));
            }

            @Override
            public void close() throws Exception {
                if (process.isAlive()) {
                    terminate();
                }
            }
        }

        @Test
        @DisplayName("valid configuration starts a listener and reports the effective settings")
        void validConfigurationStartsListening() throws Exception {
            int port = reservePort();

            try (RunningMonitor monitor =
                    new RunningMonitor(
                            Map.of("TM_BIND_ADDRESS", "127.0.0.1", "TM_PORT", String.valueOf(port)))) {
                monitor.awaitListening(port);
                monitor.awaitStdout("event=server_listening");

                String stdout = monitor.stdout();
                assertTrue(
                        stdout.contains("event=configuration"),
                        "startup should report its configuration, but stdout was: " + stdout);
                assertTrue(
                        stdout.contains("port=" + port),
                        "startup should report the configured port, but stdout was: " + stdout);
            }
        }

        @Test
        @DisplayName("invalid configuration exits non-zero with a diagnostic naming the variable")
        void invalidConfigurationExitsNonZero() throws Exception {
            ProcessResult result = launchUntilExit(Map.of("TM_PORT", "70000"));

            assertNotEquals(0, result.exitCode(), "an unusable port must fail startup");
            assertTrue(
                    result.stderr().contains("TM_PORT"),
                    "the diagnostic should name the variable, but stderr was: " + result.stderr());
        }

        @Test
        @DisplayName("a rejected log level fails startup instead of being silently ignored")
        void rejectedLogLevelExitsNonZero() throws Exception {
            ProcessResult result = launchUntilExit(Map.of("TM_LOG_LEVEL", "OFF"));

            assertNotEquals(0, result.exitCode());
            assertTrue(result.stderr().contains("TM_LOG_LEVEL"), result.stderr());
        }

        @Test
        @DisplayName("an occupied port fails startup in a real process")
        void occupiedPortExitsNonZero() throws Exception {
            try (ServerSocket occupied = new ServerSocket()) {
                occupied.bind(new InetSocketAddress("127.0.0.1", 0));

                ProcessResult result =
                        launchUntilExit(
                                Map.of(
                                        "TM_BIND_ADDRESS", "127.0.0.1",
                                        "TM_PORT", String.valueOf(occupied.getLocalPort())));

                assertNotEquals(0, result.exitCode(), "an occupied port must fail startup");
                assertTrue(
                        result.stdout().contains("event=server_start_failed"),
                        "the failure should be reported, but stdout was: " + result.stdout());
            }
        }

        @Test
        @DisplayName("the configured log level takes effect in the running process")
        void configuredLogLevelIsApplied() throws Exception {
            int infoPort = reservePort();
            try (RunningMonitor atInfo =
                    new RunningMonitor(
                            Map.of(
                                    "TM_BIND_ADDRESS", "127.0.0.1",
                                    "TM_PORT", String.valueOf(infoPort),
                                    "TM_LOG_LEVEL", "INFO"))) {
                atInfo.awaitListening(infoPort);
                atInfo.awaitStdout("event=startup");
            }

            int warnPort = reservePort();
            try (RunningMonitor atWarn =
                    new RunningMonitor(
                            Map.of(
                                    "TM_BIND_ADDRESS", "127.0.0.1",
                                    "TM_PORT", String.valueOf(warnPort),
                                    "TM_LOG_LEVEL", "WARN"))) {
                // Listening proves the process reached the point where the INFO run had already
                // logged both of its records, so silence here is suppression, not lag.
                atWarn.awaitListening(warnPort);
                assertTrue(
                        atWarn.stdout().isBlank(),
                        "WARN should suppress the INFO records, but stdout was: " + atWarn.stdout());
            }
        }

        @Test
        @DisplayName("SIGTERM uses coordinated shutdown, closes clients and releases the port")
        void sigtermRunsCoordinatedShutdown() throws Exception {
            int port = reservePort();
            try (RunningMonitor monitor =
                            new RunningMonitor(
                                    Map.of(
                                            "TM_BIND_ADDRESS", "127.0.0.1",
                                            "TM_PORT", String.valueOf(port),
                                            "TM_SHUTDOWN_GRACE_SECONDS", "3"));
                    Socket client = new Socket()) {
                monitor.awaitListening(port);
                client.connect(new InetSocketAddress("127.0.0.1", port), 5_000);
                client.getOutputStream()
                        .write(
                                ("{\"version\":1,\"sourceId\":\"sigterm-source\","
                                                + "\"occurredAt\":\"2026-09-21T18:15:42Z\","
                                                + "\"metric\":\"temperature\",\"value\":18.72}\n")
                                        .getBytes(UTF_8));
                client.getOutputStream().flush();
                monitor.awaitStdout("source_id=sigterm-source");

                long pid = monitor.pid();
                monitor.terminate();

                client.setSoTimeout((int) TimeUnit.SECONDS.toMillis(5));
                assertEquals(-1, client.getInputStream().read(), "active client socket must close");
                assertTrue(pid > 0);
                assertTrue(monitor.stdout().contains("event=shutdown_requested"), monitor.stdout());
                assertTrue(monitor.stdout().contains("event=shutdown_completed"), monitor.stdout());
                try (ServerSocket rebound = new ServerSocket()) {
                    rebound.bind(new InetSocketAddress("127.0.0.1", port));
                }
            }
        }

        private record ProcessResult(int exitCode, String stdout, String stderr) {}
    }
}
