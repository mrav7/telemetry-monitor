package io.github.mrav7.telemetrymonitor.processing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.mrav7.telemetrymonitor.configuration.ConfigurationException;
import io.github.mrav7.telemetrymonitor.configuration.ConfigurationLoader;
import io.github.mrav7.telemetrymonitor.configuration.MonitorConfiguration;
import io.github.mrav7.telemetrymonitor.protocol.ConnectionContext;
import io.github.mrav7.telemetrymonitor.protocol.TelemetryEnvelope;
import io.github.mrav7.telemetrymonitor.protocol.TelemetryEvent;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Covers the bounded queue and the fixed workers directly, where the blocking behaviour can be
 * observed without a socket in the way. The same pipeline is exercised over real TCP by
 * {@code TelemetryServerTest}.
 *
 * <p>Every wait here is bounded, and the class-level timeout means a regression fails the build
 * instead of hanging it.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class ProcessingPipelineTest {

    /** Long enough to be strong evidence of blocking, short enough to keep the suite quick. */
    private static final long BLOCKED_PROBE_MILLIS = 300;

    private static final long BOUNDED_WAIT_SECONDS = 10;

    private final ExecutorService producers = Executors.newCachedThreadPool();
    private ProcessingPipeline pipeline;

    @AfterEach
    void tearDown() {
        if (pipeline != null) {
            pipeline.close();
        }
        producers.shutdownNow();
    }

    private static MonitorConfiguration configuration(String queueCapacity, String workerThreads)
            throws ConfigurationException {
        return ConfigurationLoader.load(
                Map.of(
                        ConfigurationLoader.QUEUE_CAPACITY, queueCapacity,
                        ConfigurationLoader.WORKER_THREADS, workerThreads));
    }

    private static TelemetryEnvelope envelope(String metric) {
        return new TelemetryEnvelope(
                new TelemetryEvent(1, "source-01", Instant.parse("2026-09-21T18:15:42Z"), metric, 1.0),
                Instant.parse("2026-09-21T18:15:43Z"),
                new ConnectionContext(1, "/127.0.0.1:50000"));
    }

    @Test
    @DisplayName("queue capacity and worker count come from the configuration")
    void boundsComeFromConfiguration() throws Exception {
        pipeline = new ProcessingPipeline(configuration("7", "3"), envelope -> {});

        assertEquals(7, pipeline.queueCapacity());
        assertEquals(3, pipeline.workerCount());
    }

    @Test
    @DisplayName("a full queue blocks a started producer instead of dropping the event")
    void fullQueueBlocksTheProducer() throws Exception {
        CountDownLatch releaseProcessing = new CountDownLatch(1);
        CountDownLatch firstEventTaken = new CountDownLatch(1);
        LinkedBlockingQueue<String> processed = new LinkedBlockingQueue<>();

        pipeline =
                new ProcessingPipeline(
                        configuration("1", "1"),
                        envelope -> {
                            firstEventTaken.countDown();
                            try {
                                // Hold the single worker so the single queue slot cannot drain.
                                releaseProcessing.await(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                            processed.add(envelope.event().metric());
                        });
        pipeline.start();

        pipeline.submit(envelope("first"));
        assertTrue(
                firstEventTaken.await(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS),
                "the worker should have taken the first event");
        pipeline.submit(envelope("second"));

        // The producer signals immediately before submitting, so that the negative assertion below
        // can only be explained by the queue being full — not by the task not having run yet.
        CountDownLatch producerStarted = new CountDownLatch(1);
        Future<?> blockedProducer =
                producers.submit(
                        () -> {
                            producerStarted.countDown();
                            try {
                                pipeline.submit(envelope("third"));
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new IllegalStateException("producer was interrupted", e);
                            }
                        });

        assertTrue(
                producerStarted.await(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS),
                "the producer task should have started and reached its submission");
        assertThrows(
                TimeoutException.class,
                () -> blockedProducer.get(BLOCKED_PROBE_MILLIS, TimeUnit.MILLISECONDS),
                "the third event must not be accepted while the queue is full");
        assertFalse(blockedProducer.isDone(), "the producer must still be waiting for capacity");

        releaseProcessing.countDown();

        blockedProducer.get(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS);
        assertEquals("first", processed.poll(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS));
        assertEquals("second", processed.poll(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS));
        assertEquals(
                "third",
                processed.poll(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS),
                "the event held back by backpressure must still be processed, never dropped");
    }

    @Test
    @DisplayName("exactly the configured number of workers consume the queue")
    void workerCountIsFixed() throws Exception {
        int workers = 2;
        CountDownLatch bothWorkersBusy = new CountDownLatch(workers);
        CountDownLatch releaseProcessing = new CountDownLatch(1);
        AtomicInteger entered = new AtomicInteger();

        pipeline =
                new ProcessingPipeline(
                        configuration("8", String.valueOf(workers)),
                        envelope -> {
                            entered.incrementAndGet();
                            bothWorkersBusy.countDown();
                            try {
                                releaseProcessing.await(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });
        pipeline.start();

        for (int event = 0; event < 5; event++) {
            pipeline.submit(envelope("metric-" + event));
        }

        assertTrue(
                bothWorkersBusy.await(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS),
                "both configured workers should be processing");

        // Both workers are now held inside the boundary. Three events remain queued, so any
        // additional worker would immediately take one and raise the count above the configured
        // number; the pool cannot grow, so it stays at exactly that number.
        assertEquals(
                workers,
                entered.get(),
                "the number of concurrently processing workers must stay at the configured count");

        releaseProcessing.countDown();
    }

    @Test
    @DisplayName("a failing event does not cost the process a worker")
    void processingFailureIsIsolatedToTheEvent() throws Exception {
        LinkedBlockingQueue<String> processed = new LinkedBlockingQueue<>();

        pipeline =
                new ProcessingPipeline(
                        configuration("8", "1"),
                        envelope -> {
                            if ("poison".equals(envelope.event().metric())) {
                                throw new IllegalStateException("deliberate processing failure");
                            }
                            processed.add(envelope.event().metric());
                        });
        pipeline.start();

        pipeline.submit(envelope("poison"));
        pipeline.submit(envelope("after-failure"));

        assertEquals(
                "after-failure",
                processed.poll(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS),
                "the only worker must survive a failing event and keep consuming");
    }

    @Test
    @DisplayName("closing twice is safe and leaves no worker running")
    void closeIsIdempotent() throws Exception {
        LinkedBlockingQueue<String> processed = new LinkedBlockingQueue<>();
        pipeline =
                new ProcessingPipeline(
                        configuration("4", "2"), envelope -> processed.add(envelope.event().metric()));
        pipeline.start();
        pipeline.submit(envelope("before-close"));
        assertNotNull(processed.poll(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS));

        pipeline.close();
        pipeline.close();
    }
}
