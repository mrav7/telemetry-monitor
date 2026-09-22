package io.github.mrav7.telemetrymonitor.lifecycle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 20, unit = TimeUnit.SECONDS)
class ShutdownCoordinatorTest {

    @Test
    void shutdownWithNoWorkReachesStoppedAndIsIdempotent() {
        RecordingSteps steps = new RecordingSteps();
        AtomicInteger deadlines = new AtomicInteger();
        ShutdownCoordinator coordinator =
                new ShutdownCoordinator(
                        Duration.ofSeconds(1),
                        steps,
                        () -> {
                            deadlines.incrementAndGet();
                            return ShutdownDeadline.start(Duration.ofSeconds(1));
                        });

        coordinator.shutdown();
        ShutdownDeadline first = coordinator.deadline();
        coordinator.shutdown();

        assertEquals(ShutdownCoordinator.State.STOPPED, coordinator.state());
        assertTrue(coordinator.completion().isDone());
        assertEquals(1, deadlines.get(), "later calls must not establish another deadline");
        assertSame(first, coordinator.deadline());
        assertEquals(1, steps.stopProducers.get());
        assertEquals(1, steps.stopWorkers.get());
    }

    @Test
    void concurrentCallersJoinOneShutdownOperation() throws Exception {
        RecordingSteps steps = new RecordingSteps();
        steps.holdProducerAwait = new CountDownLatch(1);
        ShutdownCoordinator coordinator =
                new ShutdownCoordinator(
                        Duration.ofSeconds(5),
                        steps,
                        () -> ShutdownDeadline.start(Duration.ofSeconds(5)));
        ExecutorService callers = Executors.newFixedThreadPool(8);
        try {
            List<Future<?>> results = new ArrayList<>();
            for (int caller = 0; caller < 8; caller++) {
                results.add(callers.submit(coordinator::shutdown));
            }
            assertTrue(steps.producerAwaitEntered.await(5, TimeUnit.SECONDS));
            assertEquals(1, steps.stopProducers.get());
            steps.holdProducerAwait.countDown();
            for (Future<?> result : results) {
                result.get(5, TimeUnit.SECONDS);
            }
            assertEquals(1, steps.stopWorkers.get());
            assertEquals(ShutdownCoordinator.State.STOPPED, coordinator.state());
        } finally {
            callers.shutdownNow();
        }
    }

    @Test
    void producerBarrierPrecedesSchedulerAndFinalDrain() {
        RecordingSteps steps = new RecordingSteps();
        ShutdownCoordinator coordinator =
                new ShutdownCoordinator(
                        Duration.ofSeconds(1),
                        steps,
                        () -> ShutdownDeadline.start(Duration.ofSeconds(1)));

        coordinator.shutdown();

        assertEquals(
                List.of(
                        "stop-producers",
                        "await-producers",
                        "stop-scheduler",
                        "await-scheduler",
                        "stop-submissions",
                        "await-drain",
                        "stop-workers",
                        "await-workers"),
                steps.events);
    }

    @Test
    void laterPhasesReceiveOnlyTheRemainingGlobalBudget() {
        AtomicLong now = new AtomicLong();
        List<Long> observed = new ArrayList<>();
        RecordingSteps steps =
                new RecordingSteps() {
                    @Override
                    public boolean awaitProducers(ShutdownDeadline deadline) {
                        observed.add(deadline.remainingNanos());
                        now.addAndGet(60);
                        return true;
                    }

                    @Override
                    public boolean awaitScheduler(ShutdownDeadline deadline) {
                        observed.add(deadline.remainingNanos());
                        now.addAndGet(40);
                        return false;
                    }
                };
        ShutdownCoordinator coordinator =
                new ShutdownCoordinator(
                        Duration.ofNanos(100),
                        steps,
                        () -> new ShutdownDeadline(Duration.ofNanos(100), now::get));

        coordinator.shutdown();

        assertEquals(List.of(100L, 40L), observed);
        assertFalse(steps.events.contains("await-drain"), "expired budget must not be restarted");
        assertEquals(1, steps.forceScheduler.get());
        assertEquals(1, steps.forceWorkers.get());
    }

    private static class RecordingSteps implements ShutdownCoordinator.ShutdownSteps {
        final List<String> events = Collections.synchronizedList(new ArrayList<>());
        final AtomicInteger stopProducers = new AtomicInteger();
        final AtomicInteger stopWorkers = new AtomicInteger();
        final AtomicInteger forceScheduler = new AtomicInteger();
        final AtomicInteger forceWorkers = new AtomicInteger();
        final CountDownLatch producerAwaitEntered = new CountDownLatch(1);
        volatile CountDownLatch holdProducerAwait;

        @Override
        public void stopProducers() {
            events.add("stop-producers");
            stopProducers.incrementAndGet();
        }

        @Override
        public boolean awaitProducers(ShutdownDeadline deadline) throws InterruptedException {
            events.add("await-producers");
            producerAwaitEntered.countDown();
            if (holdProducerAwait != null) {
                holdProducerAwait.await();
            }
            return true;
        }

        @Override
        public void stopScheduler() {
            events.add("stop-scheduler");
        }

        @Override
        public boolean awaitScheduler(ShutdownDeadline deadline) {
            events.add("await-scheduler");
            return true;
        }

        @Override
        public void forceSchedulerStop() {
            events.add("force-scheduler");
            forceScheduler.incrementAndGet();
        }

        @Override
        public void stopSubmissions() {
            events.add("stop-submissions");
        }

        @Override
        public boolean awaitDrain(ShutdownDeadline deadline) {
            events.add("await-drain");
            return true;
        }

        @Override
        public int queueDepth() {
            return 0;
        }

        @Override
        public int inFlight() {
            return 0;
        }

        @Override
        public void stopWorkers() {
            events.add("stop-workers");
            stopWorkers.incrementAndGet();
        }

        @Override
        public boolean awaitWorkers(ShutdownDeadline deadline) {
            events.add("await-workers");
            return true;
        }

        @Override
        public int forceWorkersStop() {
            events.add("force-workers");
            forceWorkers.incrementAndGet();
            return 0;
        }
    }
}
