package io.github.mrav7.telemetrymonitor.lifecycle;

import io.github.mrav7.telemetrymonitor.network.TelemetryServer;
import io.github.mrav7.telemetrymonitor.processing.ProcessingPipeline;
import io.github.mrav7.telemetrymonitor.state.StaleMonitor;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Coordinates the service's one-way, bounded and idempotent shutdown sequence. */
public final class ShutdownCoordinator {

    public enum State {
        RUNNING,
        STOPPING,
        STOPPED
    }

    private static final Logger log = LoggerFactory.getLogger(ShutdownCoordinator.class);

    private final Duration grace;
    private final ShutdownSteps steps;
    private final Supplier<ShutdownDeadline> deadlineFactory;
    private final AtomicReference<State> state = new AtomicReference<>(State.RUNNING);
    private final CompletableFuture<Void> completion = new CompletableFuture<>();
    private volatile Thread shutdownThread;
    private volatile ShutdownDeadline deadline;

    public ShutdownCoordinator(
            Duration grace,
            TelemetryServer server,
            ProcessingPipeline pipeline,
            StaleMonitor staleMonitor) {
        this(
                grace,
                new ComponentShutdownSteps(server, pipeline, staleMonitor),
                () -> ShutdownDeadline.start(grace));
    }

    ShutdownCoordinator(
            Duration grace, ShutdownSteps steps, Supplier<ShutdownDeadline> deadlineFactory) {
        this.grace = Objects.requireNonNull(grace, "grace");
        this.steps = Objects.requireNonNull(steps, "steps");
        this.deadlineFactory = Objects.requireNonNull(deadlineFactory, "deadlineFactory");
    }

    /**
     * Runs shutdown once. Concurrent callers wait for the same completion and never create a new
     * deadline or a competing sequence.
     */
    public void shutdown() {
        if (!state.compareAndSet(State.RUNNING, State.STOPPING)) {
            if (Thread.currentThread() != shutdownThread) {
                completion.join();
            }
            return;
        }

        shutdownThread = Thread.currentThread();
        deadline = deadlineFactory.get();
        log.info("event=shutdown_requested grace_seconds={}", grace.toSeconds());
        boolean interrupted = false;
        try {
            steps.stopProducers();
            boolean producersStopped = await(steps::awaitProducers);
            interrupted |= Thread.interrupted();
            if (producersStopped) {
                log.info("event=shutdown_producers_stopped");
            } else {
                log.warn("event=shutdown_deadline_expired phase=producers");
            }

            steps.stopScheduler();
            boolean schedulerStopped = await(steps::awaitScheduler);
            interrupted |= Thread.interrupted();
            if (schedulerStopped) {
                log.info("event=shutdown_scheduler_stopped");
            } else {
                steps.forceSchedulerStop();
                log.warn("event=shutdown_forced phase=scheduler");
            }

            // A final drain is safe only after the producer termination barrier. If that barrier
            // cannot be established inside the deadline, reject further submissions and force the
            // consumers down instead of racing producers against a purported final drain.
            boolean drained = false;
            if (producersStopped) {
                steps.stopSubmissions();
                drained = await(steps::awaitDrain);
                interrupted |= Thread.interrupted();
            } else {
                steps.stopSubmissions();
            }

            if (drained) {
                log.info("event=shutdown_drain_completed");
            } else {
                log.warn(
                        "event=shutdown_deadline_expired phase=drain queue_depth={} in_flight={}",
                        steps.queueDepth(),
                        steps.inFlight());
            }

            steps.stopWorkers();
            boolean workersStopped = await(steps::awaitWorkers);
            interrupted |= Thread.interrupted();
            if (!workersStopped) {
                int discarded = steps.forceWorkersStop();
                log.warn(
                        "event=shutdown_forced phase=workers discarded_events={} queue_depth={} in_flight={}",
                        discarded,
                        steps.queueDepth(),
                        steps.inFlight());
            }
        } finally {
            state.set(State.STOPPED);
            completion.complete(null);
            log.info(
                    "event=shutdown_completed elapsed_millis={}",
                    deadline.elapsed().toMillis());
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public State state() {
        return state.get();
    }

    public CompletableFuture<Void> completion() {
        return completion;
    }

    ShutdownDeadline deadline() {
        return deadline;
    }

    private boolean await(DeadlineWait wait) {
        if (deadline.expired()) {
            return false;
        }
        try {
            return wait.await(deadline);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @FunctionalInterface
    private interface DeadlineWait {
        boolean await(ShutdownDeadline deadline) throws InterruptedException;
    }

    interface ShutdownSteps {
        void stopProducers();

        boolean awaitProducers(ShutdownDeadline deadline) throws InterruptedException;

        void stopScheduler();

        boolean awaitScheduler(ShutdownDeadline deadline) throws InterruptedException;

        void forceSchedulerStop();

        void stopSubmissions();

        boolean awaitDrain(ShutdownDeadline deadline) throws InterruptedException;

        int queueDepth();

        int inFlight();

        void stopWorkers();

        boolean awaitWorkers(ShutdownDeadline deadline) throws InterruptedException;

        int forceWorkersStop();
    }

    private record ComponentShutdownSteps(
            TelemetryServer server, ProcessingPipeline pipeline, StaleMonitor staleMonitor)
            implements ShutdownSteps {

        private ComponentShutdownSteps {
            Objects.requireNonNull(server, "server");
            Objects.requireNonNull(pipeline, "pipeline");
            Objects.requireNonNull(staleMonitor, "staleMonitor");
        }

        @Override
        public void stopProducers() {
            server.requestProducerStop();
        }

        @Override
        public boolean awaitProducers(ShutdownDeadline deadline) throws InterruptedException {
            return server.awaitProducerTermination(deadline);
        }

        @Override
        public void stopScheduler() {
            staleMonitor.requestStop();
        }

        @Override
        public boolean awaitScheduler(ShutdownDeadline deadline) throws InterruptedException {
            return staleMonitor.awaitTermination(deadline);
        }

        @Override
        public void forceSchedulerStop() {
            staleMonitor.forceStop();
        }

        @Override
        public void stopSubmissions() {
            pipeline.stopAcceptingSubmissions();
        }

        @Override
        public boolean awaitDrain(ShutdownDeadline deadline) throws InterruptedException {
            return pipeline.awaitDrained(deadline);
        }

        @Override
        public int queueDepth() {
            return pipeline.queueDepth();
        }

        @Override
        public int inFlight() {
            return pipeline.inFlightCount();
        }

        @Override
        public void stopWorkers() {
            pipeline.requestWorkerStop();
        }

        @Override
        public boolean awaitWorkers(ShutdownDeadline deadline) throws InterruptedException {
            return pipeline.awaitWorkerTermination(deadline);
        }

        @Override
        public int forceWorkersStop() {
            return pipeline.forceWorkerStop();
        }
    }
}
