package io.github.mrav7.telemetrymonitor.simulator;

import io.github.mrav7.telemetrymonitor.protocol.TelemetryMessageDecoder;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Validated command-line configuration for one simulator scenario. */
public record SimulatorConfiguration(
        String host,
        int port,
        String sourceId,
        SimulatorMode mode,
        String metric,
        double value,
        Duration interval,
        Duration duration,
        int count,
        MalformedCase malformedCase) {

    static final String DEFAULT_HOST = "127.0.0.1";
    static final int DEFAULT_PORT = 9100;
    static final String DEFAULT_METRIC = "temperature";
    static final double DEFAULT_VALUE = 20.0;
    static final double DEFAULT_RATE = 1.0;
    static final long DEFAULT_NORMAL_DURATION_SECONDS = 30;
    static final int DEFAULT_BURST_COUNT = 1000;
    static final long DEFAULT_SILENT_DURATION_SECONDS = 40;
    static final int DEFAULT_DISCONNECT_COUNT = 3;

    private static final Set<String> OPTIONS =
            Set.of(
                    "--host",
                    "--port",
                    "--source-id",
                    "--mode",
                    "--metric",
                    "--value",
                    "--rate",
                    "--duration-seconds",
                    "--count",
                    "--case");

    private static final Set<String> COMMON =
            Set.of("--host", "--port", "--source-id", "--mode", "--metric", "--value");

    /** Parses and validates the complete command line without opening a socket. */
    public static SimulatorConfiguration parse(String[] args)
            throws SimulatorConfigurationException {
        Map<String, String> supplied = parseOptions(args);
        String sourceId = required(supplied, "--source-id");
        SimulatorMode mode = SimulatorMode.parse(required(supplied, "--mode"));
        rejectInapplicableOptions(supplied.keySet(), mode);

        String host = supplied.getOrDefault("--host", DEFAULT_HOST);
        if (host.isBlank() || !host.equals(host.trim())) {
            throw new SimulatorConfigurationException("--host must be non-blank and unpadded");
        }
        int port = parseInt(supplied.getOrDefault("--port", String.valueOf(DEFAULT_PORT)), "--port");
        if (port < 1 || port > 65535) {
            throw new SimulatorConfigurationException("--port must be between 1 and 65535");
        }

        validateIdentifier(sourceId, "--source-id");
        String metric = supplied.getOrDefault("--metric", DEFAULT_METRIC);
        validateIdentifier(metric, "--metric");
        double value =
                parseFiniteDouble(
                        supplied.getOrDefault("--value", String.valueOf(DEFAULT_VALUE)), "--value");

        Duration interval = Duration.ZERO;
        Duration duration = Duration.ZERO;
        int count = 0;
        MalformedCase malformedCase = null;
        switch (mode) {
            case NORMAL -> {
                double rate =
                        parsePositiveFiniteDouble(
                                supplied.getOrDefault("--rate", String.valueOf(DEFAULT_RATE)),
                                "--rate");
                interval = intervalForRate(rate);
                duration =
                        parsePositiveDuration(
                                supplied.getOrDefault(
                                        "--duration-seconds",
                                        String.valueOf(DEFAULT_NORMAL_DURATION_SECONDS)));
            }
            case BURST ->
                    count =
                            parsePositiveInt(
                                    supplied.getOrDefault(
                                            "--count", String.valueOf(DEFAULT_BURST_COUNT)),
                                    "--count");
            case MALFORMED -> malformedCase = MalformedCase.parse(required(supplied, "--case"));
            case SILENT ->
                    duration =
                            parsePositiveDuration(
                                    supplied.getOrDefault(
                                            "--duration-seconds",
                                            String.valueOf(DEFAULT_SILENT_DURATION_SECONDS)));
            case DISCONNECT ->
                    count =
                            parsePositiveInt(
                                    supplied.getOrDefault(
                                            "--count", String.valueOf(DEFAULT_DISCONNECT_COUNT)),
                                    "--count");
        }

        return new SimulatorConfiguration(
                host, port, sourceId, mode, metric, value, interval, duration, count, malformedCase);
    }

    private static Map<String, String> parseOptions(String[] args)
            throws SimulatorConfigurationException {
        Map<String, String> supplied = new HashMap<>();
        for (int index = 0; index < args.length; index += 2) {
            String option = args[index];
            if (!OPTIONS.contains(option)) {
                throw new SimulatorConfigurationException("unknown option '" + option + "'");
            }
            if (index + 1 >= args.length || args[index + 1].startsWith("--")) {
                throw new SimulatorConfigurationException("missing value for " + option);
            }
            if (supplied.putIfAbsent(option, args[index + 1]) != null) {
                throw new SimulatorConfigurationException("duplicate option " + option);
            }
        }
        return supplied;
    }

    private static void rejectInapplicableOptions(Set<String> supplied, SimulatorMode mode)
            throws SimulatorConfigurationException {
        Set<String> applicable =
                switch (mode) {
                    case NORMAL -> Set.of("--rate", "--duration-seconds");
                    case BURST, DISCONNECT -> Set.of("--count");
                    case MALFORMED -> Set.of("--case");
                    case SILENT -> Set.of("--duration-seconds");
                };
        for (String option : supplied) {
            if (!COMMON.contains(option) && !applicable.contains(option)) {
                throw new SimulatorConfigurationException(
                        option + " does not apply to mode " + mode.cliName());
            }
        }
    }

    private static String required(Map<String, String> options, String name)
            throws SimulatorConfigurationException {
        String value = options.get(name);
        if (value == null) {
            throw new SimulatorConfigurationException("missing required option " + name);
        }
        return value;
    }

    private static void validateIdentifier(String value, String option)
            throws SimulatorConfigurationException {
        if (!TelemetryMessageDecoder.IDENTIFIER.matcher(value).matches()) {
            throw new SimulatorConfigurationException(
                    option + " must match " + TelemetryMessageDecoder.IDENTIFIER.pattern());
        }
    }

    private static int parseInt(String value, String option)
            throws SimulatorConfigurationException {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new SimulatorConfigurationException(option + " must be an integer");
        }
    }

    private static int parsePositiveInt(String value, String option)
            throws SimulatorConfigurationException {
        int parsed = parseInt(value, option);
        if (parsed <= 0) {
            throw new SimulatorConfigurationException(option + " must be positive");
        }
        return parsed;
    }

    private static double parseFiniteDouble(String value, String option)
            throws SimulatorConfigurationException {
        try {
            double parsed = Double.parseDouble(value);
            if (!Double.isFinite(parsed)) {
                throw new SimulatorConfigurationException(option + " must be finite");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new SimulatorConfigurationException(option + " must be a number");
        }
    }

    private static double parsePositiveFiniteDouble(String value, String option)
            throws SimulatorConfigurationException {
        double parsed = parseFiniteDouble(value, option);
        if (parsed <= 0) {
            throw new SimulatorConfigurationException(option + " must be positive");
        }
        return parsed;
    }

    private static Duration intervalForRate(double rate) throws SimulatorConfigurationException {
        double nanos = 1_000_000_000d / rate;
        if (nanos < 1 || nanos > Long.MAX_VALUE) {
            throw new SimulatorConfigurationException(
                    "--rate cannot be represented as a positive nanosecond interval");
        }
        return Duration.ofNanos((long) nanos);
    }

    private static Duration parsePositiveDuration(String value)
            throws SimulatorConfigurationException {
        try {
            long seconds = Long.parseLong(value);
            if (seconds <= 0) {
                throw new SimulatorConfigurationException("--duration-seconds must be positive");
            }
            Duration duration = Duration.ofSeconds(seconds);
            duration.toNanos();
            return duration;
        } catch (NumberFormatException | ArithmeticException e) {
            throw new SimulatorConfigurationException(
                    "--duration-seconds must be a representable positive integer");
        }
    }
}
