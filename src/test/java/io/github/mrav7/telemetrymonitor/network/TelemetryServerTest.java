package io.github.mrav7.telemetrymonitor.network;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.mrav7.telemetrymonitor.configuration.ConfigurationException;
import io.github.mrav7.telemetrymonitor.configuration.ConfigurationLoader;
import io.github.mrav7.telemetrymonitor.configuration.MonitorConfiguration;
import io.github.mrav7.telemetrymonitor.processing.ProcessingPipeline;
import io.github.mrav7.telemetrymonitor.protocol.TelemetryEnvelope;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Network integration tests over real loopback TCP.
 *
 * <p>The server binds port 0 through its package-private test seam, so the suite never depends on
 * a fixed port and production {@code TM_PORT} validation stays untouched. Readiness is established
 * by the bound port and by observable effects — an envelope reaching the processing boundary, a
 * socket reaching end of stream — never by sleeping for a guessed interval.
 *
 * <p>Everything the tests open is closed in {@link #tearDown()}, and the class-level timeout turns
 * a hang into a failure.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class TelemetryServerTest {

    private static final Instant MONITOR_NOW = Instant.parse("2026-09-21T18:15:42.123Z");
    private static final Clock FIXED_CLOCK = Clock.fixed(MONITOR_NOW, ZoneOffset.UTC);

    private static final long BOUNDED_WAIT_SECONDS = 15;

    /** Long enough to be evidence that nothing arrived, short enough to keep the suite quick. */
    private static final long ABSENCE_PROBE_MILLIS = 300;

    private static final String VALID_FRAME =
            "{\"version\":1,\"sourceId\":\"source-01\","
                    + "\"occurredAt\":\"2026-09-21T18:15:42.123Z\","
                    + "\"metric\":\"temperature\",\"value\":18.72}";

    private final ExecutorService serverThreads = Executors.newCachedThreadPool();
    private final List<Socket> clients = new ArrayList<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final LinkedBlockingQueue<TelemetryEnvelope> processed = new LinkedBlockingQueue<>();

    private TelemetryServer server;
    private ProcessingPipeline pipeline;
    private Future<?> acceptLoop;

    @AfterEach
    void tearDown() throws Exception {
        for (Socket client : clients) {
            closeQuietly(client);
        }
        if (server != null) {
            server.close();
        }
        if (pipeline != null) {
            pipeline.close();
        }
        if (acceptLoop != null) {
            // A server that cannot be stopped is a defect, so this wait is bounded and asserted.
            acceptLoop.get(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS);
        }
        for (AutoCloseable closeable : closeables) {
            closeQuietly(closeable);
        }
        serverThreads.shutdownNow();
        assertTrue(
                serverThreads.awaitTermination(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS),
                "test threads must not outlive the test");
    }

    // ---------------------------------------------------------------- harness

    /** Starts a server on an OS-assigned port, with the supplied configuration overrides. */
    private int startServer(String... overrides) throws Exception {
        Map<String, String> environment = new HashMap<>();
        for (int index = 0; index < overrides.length; index += 2) {
            environment.put(overrides[index], overrides[index + 1]);
        }
        MonitorConfiguration configuration = load(environment);

        pipeline = new ProcessingPipeline(configuration, processed::add);
        pipeline.start();
        server = new TelemetryServer(configuration, 0, FIXED_CLOCK, pipeline);
        server.bind();
        acceptLoop = serverThreads.submit(server::serve);
        return server.boundPort();
    }

    private static MonitorConfiguration load(Map<String, String> environment)
            throws ConfigurationException {
        return ConfigurationLoader.load(environment);
    }

    private Socket connect(int port) throws IOException {
        Socket client = new Socket();
        client.connect(new InetSocketAddress("127.0.0.1", port), 5_000);
        clients.add(client);
        return client;
    }

    private static void send(Socket client, String line) throws IOException {
        OutputStream out = client.getOutputStream();
        out.write((line + "\n").getBytes(UTF_8));
        out.flush();
    }

    private TelemetryEnvelope nextProcessed() throws InterruptedException {
        TelemetryEnvelope envelope = processed.poll(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(envelope, "an accepted message should have reached the processing boundary");
        return envelope;
    }

    private void assertNothingProcessed() throws InterruptedException {
        assertNull(
                processed.poll(ABSENCE_PROBE_MILLIS, TimeUnit.MILLISECONDS),
                "no message should have reached the processing boundary");
    }

    /**
     * Connects and keeps retrying until a client is admitted and its message is processed.
     *
     * <p>A connection slot is returned by the handler that owned it, slightly after the client
     * observes its own socket closing, so a test that frees a slot cannot assume the next connect
     * is admitted. A rejected connection is closed without producing anything, so a retry can never
     * double-count.
     */
    private void awaitAdmittedClient(int port, String sourceId, String metric) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(BOUNDED_WAIT_SECONDS);
        while (System.nanoTime() < deadline) {
            Socket candidate = connect(port);
            try {
                send(candidate, frameFor(sourceId, metric, 1.0));
                TelemetryEnvelope envelope =
                        processed.poll(ABSENCE_PROBE_MILLIS, TimeUnit.MILLISECONDS);
                if (envelope != null) {
                    assertEquals(metric, envelope.event().metric());
                    return;
                }
            } catch (IOException rejected) {
                // The server closed this connection because no slot was free yet.
            }
            closeQuietly(candidate);
        }
        throw new AssertionError("no connection slot became available for " + metric);
    }

    /** Reads until end of stream, which is how a client observes its connection being closed. */
    private static void awaitEndOfStream(Socket client) throws IOException {
        client.setSoTimeout((int) TimeUnit.SECONDS.toMillis(BOUNDED_WAIT_SECONDS));
        InputStream in = client.getInputStream();
        assertEquals(-1, in.read(), "the server should have closed this connection");
    }

    private static void closeQuietly(AutoCloseable resource) {
        try {
            resource.close();
        } catch (Exception e) {
            // Cleanup only; the test outcome is decided by the assertions.
        }
    }

    private static String frameFor(String sourceId, String metric, double value) {
        return "{\"version\":1,\"sourceId\":\"" + sourceId + "\","
                + "\"occurredAt\":\"2026-09-21T18:15:42.123Z\",\"metric\":\"" + metric
                + "\",\"value\":" + value + "}";
    }

    // ---------------------------------------------------------------- listener

    @Test
    @DisplayName("the listener binds and reports the port it was given")
    void listenerBinds() throws Exception {
        int port = startServer();

        assertTrue(port > 0, "an OS-assigned port should have been reported");
        connect(port);
    }

    @Test
    @DisplayName("binding an occupied endpoint fails instead of appearing to start")
    void bindFailureIsSurfaced() throws Exception {
        int port = startServer();

        MonitorConfiguration configuration = load(Map.of());
        ProcessingPipeline other = new ProcessingPipeline(configuration, processed::add);
        closeables.add(other);
        try (TelemetryServer competing =
                new TelemetryServer(configuration, port, FIXED_CLOCK, other)) {
            assertThrows(IOException.class, competing::bind, "the port is already listening");
        }
    }

    // ---------------------------------------------------------------- message flow

    @Test
    @DisplayName("a valid frame travels from the socket to the processing boundary")
    void validFrameReachesProcessing() throws Exception {
        int port = startServer();
        Socket client = connect(port);

        send(client, VALID_FRAME);

        TelemetryEnvelope envelope = nextProcessed();
        assertEquals("source-01", envelope.event().sourceId());
        assertEquals("temperature", envelope.event().metric());
        assertEquals(18.72, envelope.event().value());
        assertEquals(MONITOR_NOW, envelope.receivedAt(), "receivedAt comes from the monitor clock");
        assertTrue(envelope.connection().connectionId() > 0);
        assertTrue(envelope.connection().remote().contains("127.0.0.1"));
    }

    @Test
    @DisplayName("several frames on one connection all reach processing")
    void multipleFramesOnOneConnection() throws Exception {
        int port = startServer();
        Socket client = connect(port);

        send(client, frameFor("source-01", "first", 1.0));
        send(client, frameFor("source-01", "second", 2.0));
        send(client, frameFor("source-01", "third", 3.0));

        // What the service promises is that every valid frame reaches processing exactly once, not
        // the order in which the workers finish with them: the queue is drained by several workers
        // at once, so completion order is theirs to decide. Comparing sorted values rather than a
        // set keeps a duplicate delivery a failure, and the absence probe keeps an extra one a
        // failure too.
        List<String> metrics =
                new ArrayList<>(
                        List.of(
                                nextProcessed().event().metric(),
                                nextProcessed().event().metric(),
                                nextProcessed().event().metric()));
        metrics.sort(String::compareTo);

        assertEquals(List.of("first", "second", "third"), metrics);
        assertNothingProcessed();
    }

    @Test
    @DisplayName("one connection may carry several sources")
    void oneConnectionMayCarrySeveralSources() throws Exception {
        int port = startServer();
        Socket client = connect(port);

        send(client, frameFor("source-01", "temperature", 1.0));
        send(client, frameFor("source-02", "temperature", 2.0));

        // The point is that one connection is not bound to one source identity. Which of the two
        // the workers finish first is not part of that, so the values are compared sorted.
        List<String> sourceIds =
                new ArrayList<>(
                        List.of(
                                nextProcessed().event().sourceId(),
                                nextProcessed().event().sourceId()));
        sourceIds.sort(String::compareTo);

        assertEquals(List.of("source-01", "source-02"), sourceIds);
        assertNothingProcessed();
    }

    @Test
    @DisplayName("a frame split across several writes is reassembled")
    void partialWritesAreReassembled() throws Exception {
        int port = startServer();
        Socket client = connect(port);
        byte[] frame = (VALID_FRAME + "\n").getBytes(UTF_8);
        OutputStream out = client.getOutputStream();

        // Deliberately split inside the JSON so that no write boundary is a message boundary.
        int firstCut = 10;
        int secondCut = frame.length - 5;
        out.write(frame, 0, firstCut);
        out.flush();
        out.write(frame, firstCut, secondCut - firstCut);
        out.flush();
        out.write(frame, secondCut, frame.length - secondCut);
        out.flush();

        assertEquals("temperature", nextProcessed().event().metric());
    }

    @Test
    @DisplayName("CRLF line endings are accepted")
    void crlfFramesAreAccepted() throws Exception {
        int port = startServer();
        Socket client = connect(port);

        client.getOutputStream().write((VALID_FRAME + "\r\n").getBytes(UTF_8));
        client.getOutputStream().flush();

        assertEquals("temperature", nextProcessed().event().metric());
    }

    // ---------------------------------------------------------------- invalid input

    @Test
    @DisplayName("a malformed frame is discarded and the connection keeps working")
    void malformedThenValid() throws Exception {
        int port = startServer();
        Socket client = connect(port);

        send(client, "{this is not json");
        send(client, frameFor("source-01", "after-malformed", 5.0));

        TelemetryEnvelope envelope = nextProcessed();
        assertEquals(
                "after-malformed",
                envelope.event().metric(),
                "the malformed frame must not have been enqueued");
        assertNothingProcessed();
    }

    @Test
    @DisplayName("an unsupported version is rejected without ending the connection")
    void unsupportedVersionThenValid() throws Exception {
        int port = startServer();
        Socket client = connect(port);

        send(client, "{\"version\":2,\"sourceId\":\"source-01\","
                + "\"occurredAt\":\"2026-09-21T18:15:42.123Z\","
                + "\"metric\":\"temperature\",\"value\":1.0}");
        send(client, frameFor("source-01", "after-bad-version", 5.0));

        assertEquals("after-bad-version", nextProcessed().event().metric());
    }

    @Test
    @DisplayName("an empty frame is rejected without ending the connection")
    void emptyThenValid() throws Exception {
        int port = startServer();
        Socket client = connect(port);

        client.getOutputStream().write("\n".getBytes(UTF_8));
        client.getOutputStream().flush();
        send(client, frameFor("source-01", "after-empty", 4.0));

        assertEquals("after-empty", nextProcessed().event().metric());
    }

    @Test
    @DisplayName("invalid UTF-8 is rejected without ending the connection")
    void invalidUtf8ThenValid() throws Exception {
        int port = startServer();
        Socket client = connect(port);

        OutputStream out = client.getOutputStream();
        out.write(new byte[] {(byte) 0xC3, (byte) 0x28, '\n'});
        out.flush();
        send(client, frameFor("source-01", "after-invalid-utf8", 6.0));

        assertEquals("after-invalid-utf8", nextProcessed().event().metric());
    }

    @Test
    @DisplayName("an oversized frame is never enqueued and only its own connection is closed")
    void oversizedFrameIsIsolated() throws Exception {
        int port = startServer(ConfigurationLoader.MAX_MESSAGE_BYTES, "128");
        Socket offender = connect(port);
        Socket bystander = connect(port);

        // Establish that the bystander's connection is alive before the offending frame is sent.
        send(bystander, frameFor("source-02", "before", 1.0));
        assertEquals("before", nextProcessed().event().metric());

        send(offender, "{\"version\":1,\"sourceId\":\"source-01\",\"metric\":\""
                + "x".repeat(300) + "\"}");

        awaitEndOfStream(offender);
        assertNothingProcessed();

        send(bystander, frameFor("source-02", "after", 2.0));
        assertEquals(
                "after",
                nextProcessed().event().metric(),
                "an oversized frame on one connection must not disturb another");
    }

    // ---------------------------------------------------------------- connection lifecycle

    @Test
    @DisplayName("a disconnect ends only that connection")
    void disconnectDoesNotStopTheServer() throws Exception {
        int port = startServer();
        Socket first = connect(port);
        send(first, frameFor("source-01", "before-disconnect", 1.0));
        assertEquals("before-disconnect", nextProcessed().event().metric());

        first.close();

        Socket second = connect(port);
        send(second, frameFor("source-02", "after-disconnect", 2.0));
        assertEquals("after-disconnect", nextProcessed().event().metric());
    }

    @Test
    @DisplayName("several clients are served concurrently")
    void multipleClientsAreServedConcurrently() throws Exception {
        int clientCount = 8;
        int port = startServer();

        List<Socket> connected = new ArrayList<>();
        for (int index = 0; index < clientCount; index++) {
            connected.add(connect(port));
        }
        for (int index = 0; index < clientCount; index++) {
            send(connected.get(index), frameFor("source-" + index, "temperature", index));
        }

        List<String> seen = new ArrayList<>();
        for (int index = 0; index < clientCount; index++) {
            seen.add(nextProcessed().event().sourceId());
        }
        for (int index = 0; index < clientCount; index++) {
            assertTrue(seen.contains("source-" + index), "every client's message should arrive");
        }
    }

    @Test
    @DisplayName("a client stuck mid-message does not hold up another client")
    void slowClientDoesNotBlockOthers() throws Exception {
        int port = startServer();
        Socket slow = connect(port);

        // A complete frame first, so the slow client's handler is provably running and reading.
        send(slow, frameFor("source-slow", "established", 1.0));
        assertEquals("source-slow", nextProcessed().event().sourceId());

        // Now leave a frame unterminated: this handler is parked in a socket read.
        OutputStream out = slow.getOutputStream();
        out.write("{\"version\":1,\"sourceId\":\"source-slow\"".getBytes(UTF_8));
        out.flush();

        Socket fast = connect(port);
        send(fast, frameFor("source-fast", "temperature", 2.0));

        assertEquals(
                "source-fast",
                nextProcessed().event().sourceId(),
                "a client blocked mid-message must not delay an unrelated client");
    }

    // ---------------------------------------------------------------- connection capacity

    @Test
    @DisplayName("connections beyond the configured limit are rejected, and the limit is restored")
    void connectionCapacityIsEnforcedAndReleased() throws Exception {
        int port = startServer(ConfigurationLoader.MAX_CONNECTIONS, "2");

        Socket first = connect(port);
        Socket second = connect(port);
        // Both are admitted only once their handlers have read something.
        send(first, frameFor("source-01", "first", 1.0));
        send(second, frameFor("source-02", "second", 2.0));
        assertNotNull(nextProcessed());
        assertNotNull(nextProcessed());

        Socket rejected = connect(port);
        awaitEndOfStream(rejected);

        // The two admitted connections are untouched by the rejection.
        send(first, frameFor("source-01", "still-alive", 3.0));
        assertEquals("still-alive", nextProcessed().event().metric());

        // Freeing one slot lets a new client in.
        first.close();
        awaitAdmittedClient(port, "source-03", "after-release");
    }

    @Test
    @DisplayName("a connection ended by protocol failure returns its capacity")
    void capacityIsNotLeakedByAnAbnormalEnding() throws Exception {
        int port =
                startServer(
                        ConfigurationLoader.MAX_CONNECTIONS, "1",
                        ConfigurationLoader.MAX_MESSAGE_BYTES, "128");

        Socket offender = connect(port);
        send(offender, frameFor("source-01", "established", 1.0));
        assertEquals("established", nextProcessed().event().metric());

        // Oversized input leaves the stream unusable, so the server ends this connection.
        send(offender, "{\"metric\":\"" + "x".repeat(300) + "\"}");
        awaitEndOfStream(offender);

        awaitAdmittedClient(port, "source-02", "after-failure");
    }

    @Test
    @DisplayName("simultaneous connection attempts never exceed the configured limit")
    void connectionCapacityHoldsUnderSimultaneousAttempts() throws Exception {
        int limit = 2;
        int attempts = 6;
        int port = startServer(ConfigurationLoader.MAX_CONNECTIONS, String.valueOf(limit));

        // Every client task connects only once the shared gate opens, so the attempts genuinely
        // overlap instead of arriving one after another.
        CountDownLatch ready = new CountDownLatch(attempts);
        CountDownLatch start = new CountDownLatch(1);
        Map<String, Future<Socket>> pending = new LinkedHashMap<>();

        for (int index = 0; index < attempts; index++) {
            String sourceId = "contender-" + index;
            pending.put(
                    sourceId,
                    serverThreads.submit(
                            () -> {
                                Socket client = new Socket();
                                ready.countDown();
                                assertTrue(
                                        start.await(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS),
                                        "the start gate should have opened");
                                client.connect(new InetSocketAddress("127.0.0.1", port), 5_000);
                                try {
                                    send(client, frameFor(sourceId, "temperature", 1.0));
                                } catch (IOException refused) {
                                    // The server closed this connection before the frame could be
                                    // written, which is one of the two valid outcomes here.
                                }
                                return client;
                            }));
        }

        assertTrue(
                ready.await(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS),
                "every client task should be waiting on the gate before it opens");
        start.countDown();

        Map<String, Socket> sockets = new LinkedHashMap<>();
        for (Map.Entry<String, Future<Socket>> attempt : pending.entrySet()) {
            Socket client = attempt.getValue().get(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS);
            sockets.put(attempt.getKey(), client);
            clients.add(client);
        }

        // Admission is observed positively and without assuming any order: only an admitted
        // connection is ever read, so only its message can reach the processing boundary, and the
        // message names the client that sent it.
        Set<String> admitted = new LinkedHashSet<>();
        for (int delivered = 0; delivered < limit; delivered++) {
            admitted.add(nextProcessed().event().sourceId());
        }
        assertEquals(
                limit,
                admitted.size(),
                "each admitted connection should have delivered exactly one message");

        // Nothing further is admitted while the limit is held, so the invariant
        // activeConnections <= TM_MAX_CONNECTIONS survived the contention.
        assertNothingProcessed();

        // The attempts beyond capacity were closed, not parked indefinitely.
        for (Map.Entry<String, Socket> attempt : sockets.entrySet()) {
            if (!admitted.contains(attempt.getKey())) {
                awaitEndOfStream(attempt.getValue());
            }
        }

        // The admitted connections were never disturbed by the rejected attempts.
        for (String sourceId : admitted) {
            send(sockets.get(sourceId), frameFor(sourceId, "still-alive", 2.0));
        }
        Set<String> stillWorking = new LinkedHashSet<>();
        for (int delivered = 0; delivered < limit; delivered++) {
            stillWorking.add(nextProcessed().event().sourceId());
        }
        assertEquals(
                admitted,
                stillWorking,
                "the admitted connections must keep working after the excess attempts were refused");

        // Releasing the held connections returns their permits, so capacity is reusable and the
        // contention leaked nothing.
        for (String sourceId : admitted) {
            sockets.get(sourceId).close();
        }
        awaitAdmittedClient(port, "after-contention", "reused-slot");
    }

    // ---------------------------------------------------------------- stopping

    @Test
    @DisplayName("closing the server unblocks accept and frees the port")
    void closeReleasesTheListener() throws Exception {
        int port = startServer();
        connect(port);

        server.close();
        acceptLoop.get(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS);

        try (ServerSocket rebound = new ServerSocket()) {
            rebound.bind(new InetSocketAddress("127.0.0.1", port));
        }
    }

    @Test
    @DisplayName("closing the server ends a connection waiting for its next frame")
    void closeEndsAnIdleConnection() throws Exception {
        int port = startServer();
        Socket client = connect(port);
        send(client, frameFor("source-01", "established", 1.0));
        assertEquals("established", nextProcessed().event().metric());

        server.close();

        acceptLoop.get(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS);
        awaitEndOfStream(client);
    }

    @Test
    @DisplayName("closing twice is safe")
    void closeIsIdempotent() throws Exception {
        startServer();

        server.close();
        server.close();

        acceptLoop.get(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    @DisplayName("a server that never bound can still be closed, and refuses to serve")
    void unboundServerIsSafe() throws Exception {
        MonitorConfiguration configuration = load(Map.of());
        ProcessingPipeline unused = new ProcessingPipeline(configuration, processed::add);
        closeables.add(unused);
        TelemetryServer unbound = new TelemetryServer(configuration, 0, FIXED_CLOCK, unused);
        closeables.add(unbound);

        assertThrows(IllegalStateException.class, unbound::serve);
        unbound.close();
    }

    @Test
    @DisplayName("a blocked producer is released by shutdown rather than retrying")
    void stoppingReleasesAProducerWaitingForQueueCapacity() throws Exception {
        // One queue slot, one worker held inside the boundary: the connection task will block in
        // submit() and can only be freed by the server stopping.
        CountDownLatch holdWorker = new CountDownLatch(1);
        MonitorConfiguration configuration =
                load(
                        Map.of(
                                ConfigurationLoader.QUEUE_CAPACITY, "1",
                                ConfigurationLoader.WORKER_THREADS, "1"));
        pipeline =
                new ProcessingPipeline(
                        configuration,
                        envelope -> {
                            processed.add(envelope);
                            try {
                                holdWorker.await(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });
        pipeline.start();
        server = new TelemetryServer(configuration, 0, FIXED_CLOCK, pipeline);
        server.bind();
        acceptLoop = serverThreads.submit(server::serve);

        Socket client = connect(server.boundPort());
        send(client, frameFor("source-01", "taken-by-worker", 1.0));
        assertNotNull(nextProcessed());
        send(client, frameFor("source-01", "fills-queue", 2.0));
        send(client, frameFor("source-01", "blocks-producer", 3.0));

        server.close();
        acceptLoop.get(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS);
        holdWorker.countDown();
    }

    @Test
    @DisplayName("a client that sends nothing and disconnects is an ordinary end of stream")
    void silentClientDisconnect() throws Exception {
        int port = startServer();
        Socket silent = connect(port);

        silent.close();

        Socket next = connect(port);
        send(next, frameFor("source-01", "after-silent", 1.0));
        assertEquals("after-silent", nextProcessed().event().metric());
    }

    @Test
    @DisplayName("the accept loop survives a client that is reset mid-message")
    void abruptResetDoesNotStopTheServer() throws Exception {
        int port = startServer();
        Socket abrupt = connect(port);
        send(abrupt, frameFor("source-01", "before-reset", 1.0));
        assertEquals("before-reset", nextProcessed().event().metric());

        // SO_LINGER 0 makes close() send RST, so the server sees an I/O failure, not a clean EOF.
        abrupt.setSoLinger(true, 0);
        abrupt.close();

        Socket next = connect(port);
        send(next, frameFor("source-02", "after-reset", 2.0));
        assertEquals("after-reset", nextProcessed().event().metric());
    }

    @Test
    @DisplayName("a rejected connection does not consume a slot permanently")
    void rejectionDoesNotConsumeCapacity() throws Exception {
        int port = startServer(ConfigurationLoader.MAX_CONNECTIONS, "1");
        Socket admitted = connect(port);
        send(admitted, frameFor("source-01", "admitted", 1.0));
        assertEquals("admitted", nextProcessed().event().metric());

        for (int attempt = 0; attempt < 3; attempt++) {
            awaitEndOfStream(connect(port));
        }

        send(admitted, frameFor("source-01", "still-admitted", 2.0));
        assertEquals("still-admitted", nextProcessed().event().metric());
    }

    @Test
    @DisplayName("bytes left without a newline at disconnect are discarded, not decoded")
    void incompleteFrameAtEndOfStreamIsDiscarded() throws Exception {
        int port = startServer();
        Socket client = connect(port);

        client.getOutputStream().write(VALID_FRAME.getBytes(UTF_8));
        client.getOutputStream().flush();
        client.close();

        assertNothingProcessed();
    }
}
