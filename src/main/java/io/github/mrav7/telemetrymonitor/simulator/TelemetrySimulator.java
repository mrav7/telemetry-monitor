package io.github.mrav7.telemetrymonitor.simulator;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Executes one configured simulator scenario over one owned TCP connection. */
public final class TelemetrySimulator {

    static final int CONNECT_TIMEOUT_MILLIS = 5_000;

    @FunctionalInterface
    interface Waiter {
        void await(Duration duration) throws InterruptedException;
    }

    private static final Logger log = LoggerFactory.getLogger(TelemetrySimulator.class);

    private final Clock clock;
    private final TelemetryMessageEncoder encoder;
    private final Waiter waiter;
    private final LongSupplier nanoTime;

    public TelemetrySimulator() {
        this(Clock.systemUTC(), new TelemetryMessageEncoder(), Thread::sleep, System::nanoTime);
    }

    TelemetrySimulator(
            Clock clock,
            TelemetryMessageEncoder encoder,
            Waiter waiter,
            LongSupplier nanoTime) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.encoder = Objects.requireNonNull(encoder, "encoder");
        this.waiter = Objects.requireNonNull(waiter, "waiter");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
    }

    /** Runs the scenario; socket, stream and close are all owned by this call. */
    public void run(SimulatorConfiguration configuration) throws IOException, InterruptedException {
        Objects.requireNonNull(configuration, "configuration");
        log.info(
                "event=simulator_started mode={} source_id={} host={} port={}",
                configuration.mode().cliName(),
                configuration.sourceId(),
                configuration.host(),
                configuration.port());

        try (Socket socket = new Socket()) {
            socket.connect(
                    new InetSocketAddress(configuration.host(), configuration.port()),
                    CONNECT_TIMEOUT_MILLIS);
            log.info(
                    "event=simulator_connected mode={} source_id={} host={} port={}",
                    configuration.mode().cliName(),
                    configuration.sourceId(),
                    configuration.host(),
                    configuration.port());
            try (OutputStream output = new BufferedOutputStream(socket.getOutputStream())) {
                execute(configuration, output);
            }
        }

        log.info(
                "event=simulator_completed mode={} source_id={}",
                configuration.mode().cliName(),
                configuration.sourceId());
    }

    private void execute(SimulatorConfiguration configuration, OutputStream output)
            throws IOException, InterruptedException {
        switch (configuration.mode()) {
            case NORMAL -> runNormal(configuration, output);
            case BURST, DISCONNECT -> sendValid(configuration, output, configuration.count());
            case MALFORMED -> {
                output.write(
                        encoder.encodeMalformed(
                                configuration.malformedCase(),
                                configuration.sourceId(),
                                clock.instant(),
                                configuration.metric(),
                                configuration.value()));
                output.flush();
            }
            case SILENT -> {
                sendValid(configuration, output, 1);
                waiter.await(configuration.duration());
            }
        }
    }

    private void runNormal(SimulatorConfiguration configuration, OutputStream output)
            throws IOException, InterruptedException {
        long durationNanos = configuration.duration().toNanos();
        long started = nanoTime.getAsLong();
        while (elapsedSince(started) < durationNanos) {
            sendValid(configuration, output, 1);
            long remaining = durationNanos - elapsedSince(started);
            if (remaining <= 0) {
                return;
            }
            waiter.await(min(configuration.interval(), Duration.ofNanos(remaining)));
        }
    }

    private long elapsedSince(long started) {
        long elapsed = nanoTime.getAsLong() - started;
        return Math.max(0, elapsed);
    }

    private static Duration min(Duration first, Duration second) {
        return first.compareTo(second) <= 0 ? first : second;
    }

    private void sendValid(
            SimulatorConfiguration configuration, OutputStream output, int messageCount)
            throws IOException {
        for (int sent = 0; sent < messageCount; sent++) {
            output.write(
                    encoder.encode(
                            configuration.sourceId(),
                            clock.instant(),
                            configuration.metric(),
                            configuration.value()));
            output.flush();
        }
    }
}
