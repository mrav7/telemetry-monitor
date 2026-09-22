package io.github.mrav7.telemetrymonitor.simulator;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Timeout(value = 10, unit = TimeUnit.SECONDS)
class SimulatorConfigurationTest {

    @Test
    @DisplayName("normal mode applies all common and normal defaults")
    void normalDefaults() throws Exception {
        SimulatorConfiguration configuration = parse("--source-id", "source-01", "--mode", "normal");

        assertAll(
                () -> assertEquals("127.0.0.1", configuration.host()),
                () -> assertEquals(9100, configuration.port()),
                () -> assertEquals("temperature", configuration.metric()),
                () -> assertEquals(20.0, configuration.value()),
                () -> assertEquals(Duration.ofSeconds(1), configuration.interval()),
                () -> assertEquals(Duration.ofSeconds(30), configuration.duration()),
                () -> assertEquals(0, configuration.count()),
                () -> assertNull(configuration.malformedCase()));
    }

    @Test
    @DisplayName("each count or duration mode applies its documented default")
    void modeSpecificDefaults() throws Exception {
        SimulatorConfiguration burst = parse("--source-id", "s", "--mode", "burst");
        SimulatorConfiguration silent = parse("--source-id", "s", "--mode", "silent");
        SimulatorConfiguration disconnect = parse("--source-id", "s", "--mode", "disconnect");

        assertAll(
                () -> assertEquals(1000, burst.count()),
                () -> assertEquals(Duration.ofSeconds(40), silent.duration()),
                () -> assertEquals(3, disconnect.count()));
    }

    @Test
    @DisplayName("explicit common and normal values are retained")
    void explicitValues() throws Exception {
        SimulatorConfiguration configuration =
                parse(
                        "--source-id", "source_2",
                        "--mode", "normal",
                        "--host", "localhost",
                        "--port", "9123",
                        "--metric", "cpu.load",
                        "--value", "-1.25",
                        "--rate", "4",
                        "--duration-seconds", "7");

        assertAll(
                () -> assertEquals("localhost", configuration.host()),
                () -> assertEquals(9123, configuration.port()),
                () -> assertEquals("source_2", configuration.sourceId()),
                () -> assertEquals("cpu.load", configuration.metric()),
                () -> assertEquals(-1.25, configuration.value()),
                () -> assertEquals(Duration.ofMillis(250), configuration.interval()),
                () -> assertEquals(Duration.ofSeconds(7), configuration.duration()));
    }

    @Test
    @DisplayName("malformed mode requires and parses a known case")
    void malformedCase() throws Exception {
        SimulatorConfiguration configuration =
                parse(
                        "--source-id", "source-01",
                        "--mode", "malformed",
                        "--case", "invalid-value");

        assertEquals(MalformedCase.INVALID_VALUE, configuration.malformedCase());
    }

    @ParameterizedTest(name = "rejects {0}")
    @MethodSource("invalidCommands")
    void rejectsInvalidCommands(String description, String[] args) {
        assertThrows(SimulatorConfigurationException.class, () -> SimulatorConfiguration.parse(args));
    }

    private static Stream<Arguments> invalidCommands() {
        return Stream.of(
                command("missing mode", "--source-id", "s"),
                command("missing source", "--mode", "normal"),
                command("unknown option", "--source-id", "s", "--mode", "burst", "--wat", "1"),
                command("duplicate option", "--source-id", "s", "--source-id", "t", "--mode", "burst"),
                command("missing option value", "--source-id", "s", "--mode"),
                command("option followed by option", "--source-id", "--mode", "normal"),
                command("invalid port text", "--source-id", "s", "--mode", "burst", "--port", "x"),
                command("port zero", "--source-id", "s", "--mode", "burst", "--port", "0"),
                command("port too high", "--source-id", "s", "--mode", "burst", "--port", "65536"),
                command("blank host", "--source-id", "s", "--mode", "burst", "--host", " "),
                command("padded host", "--source-id", "s", "--mode", "burst", "--host", " localhost"),
                command("invalid source", "--source-id", "bad source", "--mode", "burst"),
                command("invalid metric", "--source-id", "s", "--mode", "burst", "--metric", "cpu%"),
                command("non-numeric value", "--source-id", "s", "--mode", "burst", "--value", "x"),
                command("NaN value", "--source-id", "s", "--mode", "burst", "--value", "NaN"),
                command("infinite value", "--source-id", "s", "--mode", "burst", "--value", "Infinity"),
                command("non-numeric rate", "--source-id", "s", "--mode", "normal", "--rate", "x"),
                command("zero rate", "--source-id", "s", "--mode", "normal", "--rate", "0"),
                command("negative rate", "--source-id", "s", "--mode", "normal", "--rate", "-1"),
                command("unrepresentable rate", "--source-id", "s", "--mode", "normal", "--rate", "1.1e9"),
                command("zero duration", "--source-id", "s", "--mode", "normal", "--duration-seconds", "0"),
                command("negative duration", "--source-id", "s", "--mode", "silent", "--duration-seconds", "-1"),
                command("non-integral duration", "--source-id", "s", "--mode", "silent", "--duration-seconds", "1.5"),
                command("unrepresentable duration", "--source-id", "s", "--mode", "normal", "--duration-seconds", "9223372036854775807"),
                command("non-numeric count", "--source-id", "s", "--mode", "burst", "--count", "x"),
                command("zero count", "--source-id", "s", "--mode", "burst", "--count", "0"),
                command("negative count", "--source-id", "s", "--mode", "disconnect", "--count", "-1"),
                command("invalid mode", "--source-id", "s", "--mode", "fast"),
                command("invalid malformed case", "--source-id", "s", "--mode", "malformed", "--case", "raw"),
                command("missing malformed case", "--source-id", "s", "--mode", "malformed"),
                command("burst rate", "--source-id", "s", "--mode", "burst", "--rate", "10"),
                command("silent count", "--source-id", "s", "--mode", "silent", "--count", "2"),
                command("normal malformed case", "--source-id", "s", "--mode", "normal", "--case", "broken-json"),
                command("malformed duration", "--source-id", "s", "--mode", "malformed", "--case", "broken-json", "--duration-seconds", "1"));
    }

    private static Arguments command(String description, String... args) {
        return Arguments.of(description, args);
    }

    private static SimulatorConfiguration parse(String... args)
            throws SimulatorConfigurationException {
        return SimulatorConfiguration.parse(args);
    }
}
