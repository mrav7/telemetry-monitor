package io.github.mrav7.telemetrymonitor.processing;

import io.github.mrav7.telemetrymonitor.configuration.MonitorConfiguration;
import io.github.mrav7.telemetrymonitor.protocol.TelemetryEnvelope;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decouples ingestion from processing through a bounded queue drained by a fixed set of workers.
 *
 * <p>The queue is bounded and the worker count is fixed, so the memory and thread cost of the
 * service does not grow with load. When the queue is full, {@link #submit(TelemetryEnvelope)}
 * blocks instead of discarding: the connection thread stops reading its socket, and TCP then slows
 * the remote sender. No valid event is dropped during normal operation.
 *
 * <p>Workers hand each envelope to a {@link Consumer} boundary. That boundary is where source state
 * will later be applied; this class deliberately holds no state of its own beyond the queue.
 *
 * <p>The pipeline owns its queue and its worker executor, and closes both in {@link #close()}.
 */
public final class ProcessingPipeline implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ProcessingPipeline.class);

    /**
     * Bound on waiting for workers to notice {@link #close()}. This is only enough to keep tests
     * and local runs from leaking threads; the service-wide shutdown deadline is separate work and
     * is not implemented here.
     */
    private static final long TERMINATION_TIMEOUT_SECONDS = 5;

    private final BlockingQueue<TelemetryEnvelope> queue;
    private final ExecutorService workers;
    private final Consumer<TelemetryEnvelope> boundary;
    private final int queueCapacity;
    private final int workerCount;

    /**
     * @param configuration supplies the frozen queue capacity and worker count
     * @param boundary      invoked once per envelope, on a worker thread
     */
    public ProcessingPipeline(
            MonitorConfiguration configuration, Consumer<TelemetryEnvelope> boundary) {
        Objects.requireNonNull(configuration, "configuration");
        this.boundary = Objects.requireNonNull(boundary, "boundary");
        this.queueCapacity = configuration.queueCapacity();
        this.workerCount = configuration.workerThreads();
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        ThreadFactory factory = Thread.ofPlatform().name("tm-worker-", 1).daemon(false).factory();
        this.workers = Executors.newFixedThreadPool(workerCount, factory);
    }

    /** Starts exactly the configured number of workers. */
    public void start() {
        for (int worker = 0; worker < workerCount; worker++) {
            workers.execute(this::consume);
        }
        log.debug("event=processing_started workers={} queue_capacity={}", workerCount, queueCapacity);
    }

    /**
     * Hands a validated envelope to the queue, waiting while the queue is full.
     *
     * @throws InterruptedException if the calling thread is interrupted while waiting; the caller
     *     must stop producing rather than retry, so that a stopping service cannot keep enqueueing
     */
    public void submit(TelemetryEnvelope envelope) throws InterruptedException {
        queue.put(Objects.requireNonNull(envelope, "envelope"));
    }

    public int queueCapacity() {
        return queueCapacity;
    }

    public int workerCount() {
        return workerCount;
    }

    private void consume() {
        while (true) {
            TelemetryEnvelope envelope;
            try {
                envelope = queue.take();
            } catch (InterruptedException e) {
                // The pipeline is closing. Restore the flag and let the worker end.
                Thread.currentThread().interrupt();
                return;
            }
            apply(envelope);
        }
    }

    private void apply(TelemetryEnvelope envelope) {
        try {
            boundary.accept(envelope);
        } catch (RuntimeException e) {
            // One defective event must not silently remove a worker for the rest of the process,
            // so the failure is reported and the worker continues with the next envelope.
            log.error(
                    "event=processing_failure source_id={} metric={} reason={}",
                    envelope.event().sourceId(),
                    envelope.event().metric(),
                    e.toString(),
                    e);
        }
    }

    /** Stops the workers and releases the executor. Safe to call more than once. */
    @Override
    public void close() {
        workers.shutdownNow();
        try {
            if (!workers.awaitTermination(TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.warn("event=processing_workers_not_terminated timeout_seconds={}",
                        TERMINATION_TIMEOUT_SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
