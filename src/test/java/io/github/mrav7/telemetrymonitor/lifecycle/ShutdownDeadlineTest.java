package io.github.mrav7.telemetrymonitor.lifecycle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class ShutdownDeadlineTest {

    @Test
    void remainingBudgetOnlyDecreasesFromTheOriginalDeadline() {
        AtomicLong now = new AtomicLong(1_000);
        ShutdownDeadline deadline =
                new ShutdownDeadline(Duration.ofNanos(100), now::get);

        assertEquals(Duration.ofNanos(100), deadline.remaining());
        now.addAndGet(60);
        assertEquals(Duration.ofNanos(40), deadline.remaining());
        assertFalse(deadline.expired());
        now.addAndGet(40);
        assertEquals(Duration.ZERO, deadline.remaining());
        assertTrue(deadline.expired());
        now.addAndGet(20);
        assertEquals(Duration.ofNanos(120), deadline.elapsed());
    }
}
