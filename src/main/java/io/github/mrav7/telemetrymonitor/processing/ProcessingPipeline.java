package io.github.mrav7.telemetrymonitor.processing;

import io.github.mrav7.telemetrymonitor.configuration.MonitorConfiguration;
import io.github.mrav7.telemetrymonitor.lifecycle.ShutdownDeadline;
import io.github.mrav7.telemetrymonitor.protocol.TelemetryEnvelope;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
 * <p>Workers hand each envelope to a {@link Consumer} boundary, which the service wires to the
 * source registry. This class deliberately holds no state of its own beyond the queue, so what an
 * event does to source state is decided in one place and not here.
 *
 * <p>The pipeline owns its queue and its worker executor. During coordinated shutdown it also
 * tracks accepted work through the processing boundary, so an empty queue is not mistaken for a
 * completed drain while a worker is still applying an event.
 */
public final class ProcessingPipeline implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ProcessingPipeline.class);

    /**
     * Fallback bound used only by {@link #close()}. Coordinated service shutdown passes the shared
     * remaining deadline to the explicit drain and termination methods instead.
     */
    private static final long TERMINATION_TIMEOUT_SECONDS = 5;

    private final BlockingQueue<TelemetryEnvelope> queue;
    private final ExecutorService workers;
    private final Consumer<TelemetryEnvelope> boundary;
    private final int queueCapacity;
    private final int workerCount;
    private final AtomicBoolean acceptingSubmissions = new AtomicBoolean(true);
    private final AtomicInteger outstanding = new AtomicInteger();
    private final AtomicInteger inFlight = new AtomicInteger();
    private final Object drainMonitor = new Object();

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
        Objects.requireNonNull(envelope, "envelope");
        if (!acceptingSubmissions.get()) {
            throw new InterruptedException("processing pipeline is stopping");
        }

        // Count before put so a fast worker cannot finish the envelope before it is represented in
        // the drain state. An interrupted put rolls the reservation back and is never retried.
        outstanding.incrementAndGet();
        boolean submitted = false;
        try {
            queue.put(envelope);
            submitted = true;
        } finally {
            if (!submitted) {
                workCompleted();
            }
        }
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
            inFlight.incrementAndGet();
            try {
                apply(envelope);
            } finally {
                inFlight.decrementAndGet();
                workCompleted();
            }
        }
    }

    /** Seals the queue after the coordinator has established the producer termination barrier. */
    public void stopAcceptingSubmissions() {
        acceptingSubmissions.set(false);
    }

    /**
     * Waits until no accepted envelope is queued or executing, using only the global deadline.
     */
    public boolean awaitDrained(ShutdownDeadline deadline) throws InterruptedException {
        Objects.requireNonNull(deadline, "deadline");
        synchronized (drainMonitor) {
            while (!drained()) {
                long remaining = deadline.remainingNanos();
                if (remaining == 0L) {
                    return false;
                }
                TimeUnit.NANOSECONDS.timedWait(drainMonitor, remaining);
            }
            return true;
        }
    }

    public int queueDepth() {
        return queue.size();
    }

    public int inFlightCount() {
        return inFlight.get();
    }

    /** Accepted work that is queued, in flight, or currently blocked while entering the queue. */
    public int outstandingWorkCount() {
        return outstanding.get();
    }

    /** Requests clean worker termination after final drain. */
    public void requestWorkerStop() {
        workers.shutdownNow();
    }

    public boolean awaitWorkerTermination(ShutdownDeadline deadline) throws InterruptedException {
        return deadline.awaitTermination(workers);
    }

    /** Interrupts workers and discards queued, non-durable work. */
    public int forceWorkerStop() {
        acceptingSubmissions.set(false);
        int discarded = queue.size();
        queue.clear();
        workers.shutdownNow();
        return discarded;
    }

    private boolean drained() {
        return outstanding.get() == 0 && queue.isEmpty() && inFlight.get() == 0;
    }

    private void workCompleted() {
        int remaining = outstanding.decrementAndGet();
        if (remaining < 0) {
            throw new IllegalStateException("processing work count became negative");
        }
        if (remaining == 0) {
            synchronized (drainMonitor) {
                drainMonitor.notifyAll();
            }
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
        stopAcceptingSubmissions();
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
