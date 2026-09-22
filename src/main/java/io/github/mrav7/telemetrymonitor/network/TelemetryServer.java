package io.github.mrav7.telemetrymonitor.network;

import io.github.mrav7.telemetrymonitor.configuration.MonitorConfiguration;
import io.github.mrav7.telemetrymonitor.processing.ProcessingPipeline;
import io.github.mrav7.telemetrymonitor.protocol.BoundedFrameReader;
import io.github.mrav7.telemetrymonitor.protocol.ConnectionContext;
import io.github.mrav7.telemetrymonitor.protocol.FrameReadResult;
import io.github.mrav7.telemetrymonitor.protocol.MessageRejectedException;
import io.github.mrav7.telemetrymonitor.protocol.RejectionReason;
import io.github.mrav7.telemetrymonitor.protocol.TelemetryEnvelope;
import io.github.mrav7.telemetrymonitor.protocol.TelemetryEvent;
import io.github.mrav7.telemetrymonitor.protocol.TelemetryMessageDecoder;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Clock;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Accepts telemetry connections and turns their byte streams into validated envelopes.
 *
 * <p>The accept loop stays on the calling thread and hands every admitted connection to its own
 * virtual thread, so a client that sends slowly, or that stops mid-message, delays nobody else.
 * Admission is bounded by {@code TM_MAX_CONNECTIONS}: the permit is taken before the connection is
 * handed over and returned exactly once when the connection task ends, so the number of active
 * connections cannot exceed the limit even when clients arrive simultaneously. A connection that
 * arrives with no capacity left is closed immediately; connections already running are untouched.
 *
 * <p>Each connection task does only {@code read → frame → decode → validate → enqueue}. It holds no
 * state across connections and knows nothing about sources.
 *
 * <p>Ownership: this class creates and closes the listening socket, the connection executor, and
 * every accepted socket. The processing pipeline is supplied by the caller, which also closes it.
 * Accepted sockets are tracked so that closing the server closes them explicitly, which keeps
 * ownership obvious and termination deterministic rather than dependent on interrupt delivery.
 */
public final class TelemetryServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TelemetryServer.class);

    /**
     * Bound on waiting for connection tasks after {@link #close()}. Enough to keep runs and tests
     * from leaking threads; the service-wide shutdown deadline is separate work, not implemented
     * here.
     */
    private static final long TERMINATION_TIMEOUT_SECONDS = 5;

    private final MonitorConfiguration configuration;
    private final int listenPort;
    private final Clock clock;
    private final ProcessingPipeline pipeline;
    private final TelemetryMessageDecoder decoder = new TelemetryMessageDecoder();

    private final Semaphore connectionPermits;

    /**
     * Sockets of the connections currently admitted, so that {@link #close()} can close what this
     * server opened without relying on interrupt delivery to reach each connection task.
     */
    private final Set<Socket> activeSockets = ConcurrentHashMap.newKeySet();
    private final ExecutorService connections = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicLong connectionIds = new AtomicLong();

    private volatile ServerSocket listener;
    private volatile boolean stopping;

    /** Builds a server that listens on the configured endpoint. */
    public TelemetryServer(
            MonitorConfiguration configuration, Clock clock, ProcessingPipeline pipeline) {
        this(configuration, configuration.port(), clock, pipeline);
    }

    /**
     * Test seam: binds the supplied port instead of {@code TM_PORT}, so the suite can ask the OS
     * for a free port without the production configuration contract having to accept port 0.
     */
    TelemetryServer(
            MonitorConfiguration configuration,
            int listenPort,
            Clock clock,
            ProcessingPipeline pipeline) {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.listenPort = listenPort;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.pipeline = Objects.requireNonNull(pipeline, "pipeline");
        this.connectionPermits = new Semaphore(configuration.maxConnections());
    }

    /**
     * Opens the listening socket.
     *
     * @throws IOException if the endpoint cannot be bound, including when the port is already in
     *     use; startup must not continue as if the service were reachable
     */
    public void bind() throws IOException {
        ServerSocket socket = new ServerSocket();
        try {
            socket.bind(new InetSocketAddress(configuration.bindAddress(), listenPort));
        } catch (IOException e) {
            socket.close();
            throw e;
        }
        listener = socket;
        log.info(
                "event=server_listening bind_address={} port={} max_connections={}",
                configuration.bindAddress().getHostAddress(),
                socket.getLocalPort(),
                configuration.maxConnections());
    }

    /** The port actually bound, which is the OS-assigned one when the test seam asked for 0. */
    public int boundPort() {
        return requireBound().getLocalPort();
    }

    /**
     * Accepts connections until the server is closed.
     *
     * <p>Blocks the calling thread. Failures that belong to one connection never reach this loop;
     * they end that connection's task instead.
     */
    public void serve() {
        ServerSocket current = requireBound();
        while (!stopping) {
            Socket socket;
            try {
                socket = current.accept();
            } catch (IOException e) {
                if (stopping || current.isClosed()) {
                    break;
                }
                // A failure to accept one connection is not a reason to stop serving the others.
                log.warn("event=accept_failed reason={}", e.toString());
                continue;
            }
            admit(socket);
        }
        log.debug("event=accept_loop_ended");
    }

    private void admit(Socket socket) {
        if (!connectionPermits.tryAcquire()) {
            log.warn(
                    "event=connection_rejected reason=max_connections remote={} limit={}",
                    remoteOf(socket),
                    configuration.maxConnections());
            closeQuietly(socket);
            return;
        }

        long connectionId = connectionIds.incrementAndGet();
        boolean handed = false;
        try {
            activeSockets.add(socket);
            connections.execute(new Connection(socket, connectionId));
            handed = true;
        } catch (RejectedExecutionException e) {
            // The executor is shutting down, so nothing will run this connection.
            log.debug("event=connection_dropped connection_id={} reason=stopping", connectionId);
        } finally {
            if (!handed) {
                release(socket);
                closeQuietly(socket);
            }
        }
    }

    /** Returns the permit and forgets the socket. Called exactly once per admitted connection. */
    private void release(Socket socket) {
        activeSockets.remove(socket);
        connectionPermits.release();
    }

    /**
     * Stops accepting, ends active connections and releases every resource this server created.
     *
     * <p>Each blocked operation is released by the mechanism that actually reaches it. The accept
     * loop runs on the caller's thread, which is a platform thread, so the listening socket is
     * closed to release it. Connection tasks run on virtual threads, where a blocking read on the
     * system-default socket implementation <em>is</em> interruptible — interrupting such a thread
     * wakes the read and closes its socket — and a task waiting for queue capacity is interruptible
     * in the ordinary way; {@code shutdownNow()} covers both. The active sockets are nevertheless
     * closed here explicitly, so that termination does not depend on interrupt delivery and this
     * class visibly closes what it opened. Safe to call repeatedly, and safe to call when the
     * server never bound.
     *
     * <p>This is resource termination, not the service's shutdown policy: there is no deadline, no
     * queue drain and no guarantee about events already accepted.
     */
    @Override
    public void close() {
        stopping = true;

        ServerSocket current = listener;
        if (current != null) {
            closeQuietly(current);
        }
        for (Socket socket : activeSockets) {
            closeQuietly(socket);
        }

        connections.shutdownNow();
        try {
            if (!connections.awaitTermination(TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.warn(
                        "event=connections_not_terminated timeout_seconds={}",
                        TERMINATION_TIMEOUT_SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("event=server_stopped");
    }

    private ServerSocket requireBound() {
        ServerSocket current = listener;
        if (current == null) {
            throw new IllegalStateException("server is not bound; call bind() first");
        }
        return current;
    }

    private static String remoteOf(Socket socket) {
        return String.valueOf(socket.getRemoteSocketAddress());
    }

    private static void closeQuietly(java.io.Closeable resource) {
        try {
            resource.close();
        } catch (IOException e) {
            log.debug("event=close_failed resource={} reason={}", resource.getClass().getSimpleName(),
                    e.toString());
        }
    }

    /**
     * One client connection: reads frames until the stream ends, the protocol makes the stream
     * unusable, or the server stops.
     */
    private final class Connection implements Runnable {

        private final Socket socket;
        private final ConnectionContext context;

        private Connection(Socket socket, long connectionId) {
            this.socket = socket;
            this.context = new ConnectionContext(connectionId, remoteOf(socket));
        }

        @Override
        public void run() {
            log.debug("event=connection_accepted {}", context);
            try (Socket owned = socket;
                    InputStream in = new BufferedInputStream(owned.getInputStream())) {
                consume(new BoundedFrameReader(in, configuration.maxMessageBytes()));
            } catch (IOException e) {
                // Isolated to this connection: the listener and the workers are unaffected.
                if (!stopping) {
                    log.warn("event=connection_io_failure {} reason={}", context, e.toString());
                }
            } catch (InterruptedException e) {
                // The server is stopping while this connection waited for queue capacity. The
                // envelope is not re-enqueued: a stopping service must stop producing.
                Thread.currentThread().interrupt();
                log.debug("event=connection_interrupted {}", context);
            } finally {
                release(socket);
                log.debug("event=connection_closed {}", context);
            }
        }

        private void consume(BoundedFrameReader reader) throws IOException, InterruptedException {
            while (true) {
                FrameReadResult result = reader.readFrame();
                switch (result) {
                    case FrameReadResult.EndOfStream eof -> {
                        log.debug(
                                "event=connection_end_of_stream {} incomplete_frame_discarded={}",
                                context,
                                eof.incompleteFrameDiscarded());
                        return;
                    }
                    case FrameReadResult.Rejected rejected -> {
                        if (!report(rejected.reason())) {
                            return;
                        }
                    }
                    case FrameReadResult.Frame frame -> {
                        TelemetryEvent event;
                        try {
                            event = decoder.decode(frame.payload());
                        } catch (MessageRejectedException e) {
                            if (!report(e.reason())) {
                                return;
                            }
                            continue;
                        }
                        pipeline.submit(TelemetryEnvelope.receivedNow(event, clock, context));
                    }
                }
            }
        }

        /**
         * Logs one rejected message and reports whether the connection can carry on. The payload
         * itself is never logged: the reason is what an operator needs, and untrusted bytes in the
         * log are a liability.
         */
        private boolean report(RejectionReason reason) {
            String event =
                    reason == RejectionReason.MESSAGE_TOO_LARGE
                            ? "oversized_message"
                            : "message_rejected";
            log.warn(
                    "event={} {} reason={}",
                    event,
                    context,
                    reason.name().toLowerCase(Locale.ROOT));
            return !reason.terminatesConnection();
        }
    }
}
