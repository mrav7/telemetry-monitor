package io.github.mrav7.telemetrymonitor.simulator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.mrav7.telemetrymonitor.protocol.MessageRejectedException;
import io.github.mrav7.telemetrymonitor.protocol.RejectionReason;
import io.github.mrav7.telemetrymonitor.protocol.TelemetryEvent;
import io.github.mrav7.telemetrymonitor.protocol.TelemetryMessageDecoder;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Timeout(value = 15, unit = TimeUnit.SECONDS)
class TelemetrySimulatorTest {

    private static final Instant NOW = Instant.parse("2026-09-22T12:34:56Z");
    private static final Clock FIXED_CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    private static final Duration FUTURE_TIMEOUT = Duration.ofSeconds(5);

    private final TelemetryMessageDecoder decoder = new TelemetryMessageDecoder();

    @Test
    @DisplayName("normal mode sends repeated valid frames over one connection for its duration")
    void normalMode() throws Exception {
        AtomicLong nanos = new AtomicLong();
        TelemetrySimulator simulator =
                new TelemetrySimulator(
                        FIXED_CLOCK,
                        new TelemetryMessageEncoder(),
                        duration -> nanos.addAndGet(duration.toNanos()),
                        nanos::get);

        try (CaptureServer capture = new CaptureServer()) {
            SimulatorConfiguration configuration =
                    configuration(
                            capture.port(),
                            "normal",
                            "--rate",
                            "10",
                            "--duration-seconds",
                            "1");

            simulator.run(configuration);
            List<byte[]> frames = capture.awaitFrames();

            assertTrue(frames.size() > 1);
            assertEquals(10, frames.size());
            assertValidFrames(frames, "capture-source");
        }
    }

    @Test
    @DisplayName("burst mode sends exactly the requested valid frames and reaches EOF")
    void burstMode() throws Exception {
        try (CaptureServer capture = new CaptureServer()) {
            SimulatorConfiguration configuration =
                    configuration(capture.port(), "burst", "--count", "17");

            productionLikeSimulator().run(configuration);
            List<byte[]> frames = capture.awaitFrames();

            assertEquals(17, frames.size());
            assertValidFrames(frames, "capture-source");
        }
    }

    @ParameterizedTest(name = "malformed {0} sends one frame rejected as {1}")
    @MethodSource("malformedCases")
    void malformedMode(MalformedCase malformedCase, RejectionReason expected) throws Exception {
        try (CaptureServer capture = new CaptureServer()) {
            SimulatorConfiguration configuration =
                    configuration(
                            capture.port(),
                            "malformed",
                            "--case",
                            malformedCase.cliName());

            productionLikeSimulator().run(configuration);
            List<byte[]> frames = capture.awaitFrames();

            assertEquals(1, frames.size());
            MessageRejectedException rejection =
                    assertThrows(
                            MessageRejectedException.class,
                            () -> decoder.decode(withoutLf(frames.getFirst())));
            assertEquals(expected, rejection.reason());
        }
    }

    @Test
    @DisplayName("silent mode flushes one frame and keeps the connection open until released")
    void silentMode() throws Exception {
        CountDownLatch waitEntered = new CountDownLatch(1);
        CountDownLatch releaseWait = new CountDownLatch(1);
        TelemetrySimulator simulator =
                new TelemetrySimulator(
                        FIXED_CLOCK,
                        new TelemetryMessageEncoder(),
                        ignored -> {
                            waitEntered.countDown();
                            if (!releaseWait.await(5, TimeUnit.SECONDS)) {
                                throw new InterruptedException("test did not release silent wait");
                            }
                        },
                        System::nanoTime);

        try (SilentCapture capture = new SilentCapture();
                ExecutorService executor = Executors.newSingleThreadExecutor()) {
            SimulatorConfiguration configuration =
                    configuration(capture.port(), "silent", "--duration-seconds", "1");
            Future<?> execution = executor.submit(() -> runUnchecked(simulator, configuration));

            assertTrue(waitEntered.await(5, TimeUnit.SECONDS));
            capture.awaitNoSecondFrameWhileOpen();
            assertFalse(execution.isDone(), "silent scenario closed before its wait was released");

            releaseWait.countDown();
            execution.get(FUTURE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertEquals(1, capture.awaitFrames().size());
            assertValidFrames(capture.awaitFrames(), "capture-source");
        } finally {
            releaseWait.countDown();
        }
    }

    @Test
    @DisplayName("disconnect mode sends its configured valid frames then closes normally")
    void disconnectMode() throws Exception {
        try (CaptureServer capture = new CaptureServer()) {
            SimulatorConfiguration configuration =
                    configuration(capture.port(), "disconnect", "--count", "4");

            productionLikeSimulator().run(configuration);
            List<byte[]> frames = capture.awaitFrames();

            assertEquals(4, frames.size());
            assertValidFrames(frames, "capture-source");
        }
    }

    private TelemetrySimulator productionLikeSimulator() {
        return new TelemetrySimulator(
                FIXED_CLOCK, new TelemetryMessageEncoder(), Thread::sleep, System::nanoTime);
    }

    private void assertValidFrames(List<byte[]> frames, String sourceId) throws Exception {
        for (byte[] frame : frames) {
            assertEquals((byte) '\n', frame[frame.length - 1]);
            TelemetryEvent event = decoder.decode(withoutLf(frame));
            assertEquals(TelemetryMessageDecoder.SUPPORTED_VERSION, event.version());
            assertEquals(sourceId, event.sourceId());
            assertEquals(NOW, event.occurredAt());
            assertEquals("temperature", event.metric());
            assertEquals(20.0, event.value());
        }
    }

    private static SimulatorConfiguration configuration(int port, String mode, String... additions)
            throws SimulatorConfigurationException {
        List<String> args =
                new ArrayList<>(
                        List.of(
                                "--host",
                                "127.0.0.1",
                                "--port",
                                String.valueOf(port),
                                "--source-id",
                                "capture-source",
                                "--mode",
                                mode));
        args.addAll(List.of(additions));
        return SimulatorConfiguration.parse(args.toArray(String[]::new));
    }

    private static void runUnchecked(
            TelemetrySimulator simulator, SimulatorConfiguration configuration) {
        try {
            simulator.run(configuration);
        } catch (IOException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] withoutLf(byte[] frame) {
        return Arrays.copyOf(frame, frame.length - 1);
    }

    private static Stream<Arguments> malformedCases() {
        return Stream.of(
                Arguments.of(MalformedCase.BROKEN_JSON, RejectionReason.INVALID_JSON),
                Arguments.of(MalformedCase.MISSING_FIELD, RejectionReason.MISSING_FIELD),
                Arguments.of(MalformedCase.UNSUPPORTED_VERSION, RejectionReason.UNSUPPORTED_VERSION),
                Arguments.of(MalformedCase.INVALID_SOURCE_ID, RejectionReason.INVALID_SOURCE_ID),
                Arguments.of(MalformedCase.INVALID_TIMESTAMP, RejectionReason.INVALID_TIMESTAMP),
                Arguments.of(MalformedCase.INVALID_VALUE, RejectionReason.INVALID_VALUE));
    }

    private static List<byte[]> readFrames(Socket socket) throws IOException {
        socket.setSoTimeout(5_000);
        List<byte[]> frames = new ArrayList<>();
        ByteArrayOutputStream current = new ByteArrayOutputStream();
        InputStream input = socket.getInputStream();
        int next;
        while ((next = input.read()) != -1) {
            current.write(next);
            if (next == '\n') {
                frames.add(current.toByteArray());
                current.reset();
            }
        }
        if (current.size() != 0) {
            throw new IOException("capture ended with an unterminated frame");
        }
        return List.copyOf(frames);
    }

    private static class CaptureServer implements AutoCloseable {

        final ServerSocket listener;
        final ExecutorService executor;
        final Future<List<byte[]>> captured;

        CaptureServer() throws IOException {
            listener = new ServerSocket();
            listener.bind(new InetSocketAddress("127.0.0.1", 0));
            executor = Executors.newSingleThreadExecutor();
            captured =
                    executor.submit(
                            () -> {
                                try (Socket socket = listener.accept()) {
                                    return readFrames(socket);
                                }
                            });
        }

        int port() {
            return listener.getLocalPort();
        }

        List<byte[]> awaitFrames() throws Exception {
            return captured.get(FUTURE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public void close() throws Exception {
            listener.close();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private static final class SilentCapture implements AutoCloseable {

        private final ServerSocket listener;
        private final ExecutorService executor;
        private final Future<List<byte[]>> captured;
        private final CountDownLatch noSecondFrameObserved = new CountDownLatch(1);

        SilentCapture() throws IOException {
            listener = new ServerSocket();
            listener.bind(new InetSocketAddress("127.0.0.1", 0));
            executor = Executors.newSingleThreadExecutor();
            captured = executor.submit(this::captureSilence);
        }

        int port() {
            return listener.getLocalPort();
        }

        void awaitNoSecondFrameWhileOpen() throws Exception {
            assertTrue(noSecondFrameObserved.await(5, TimeUnit.SECONDS));
        }

        List<byte[]> awaitFrames() throws Exception {
            return captured.get(FUTURE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }

        private List<byte[]> captureSilence() throws IOException {
            try (Socket socket = listener.accept()) {
                InputStream input = socket.getInputStream();
                byte[] first = readOneFrame(input, socket);
                socket.setSoTimeout(200);
                try {
                    int unexpected = input.read();
                    if (unexpected == -1) {
                        throw new IOException("silent connection reached EOF before wait completed");
                    }
                    throw new IOException("silent mode sent a second frame");
                } catch (SocketTimeoutException expectedSilence) {
                    noSecondFrameObserved.countDown();
                }

                socket.setSoTimeout(5_000);
                if (input.read() != -1) {
                    throw new IOException("silent mode sent data after its quiet interval");
                }
                return List.of(first);
            }
        }

        private static byte[] readOneFrame(InputStream input, Socket socket) throws IOException {
            socket.setSoTimeout(5_000);
            ByteArrayOutputStream frame = new ByteArrayOutputStream();
            int next;
            while ((next = input.read()) != -1) {
                frame.write(next);
                if (next == '\n') {
                    return frame.toByteArray();
                }
            }
            throw new IOException("silent connection ended before its first frame");
        }

        @Override
        public void close() throws Exception {
            listener.close();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}
