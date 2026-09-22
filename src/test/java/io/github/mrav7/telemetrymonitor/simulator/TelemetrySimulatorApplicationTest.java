package io.github.mrav7.telemetrymonitor.simulator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TelemetrySimulatorApplicationTest {

    private static final String[] VALID = {"--source-id", "source-01", "--mode", "burst"};

    @Test
    @DisplayName("invalid configuration returns 1 before execution")
    void invalidConfiguration() {
        AtomicBoolean invoked = new AtomicBoolean();

        int result =
                TelemetrySimulatorApplication.run(
                        new String[] {"--mode", "burst"}, ignored -> invoked.set(true));

        assertEquals(TelemetrySimulatorApplication.EXIT_INVALID_CONFIGURATION, result);
        assertFalse(invoked.get());
    }

    @Test
    @DisplayName("successful execution returns 0")
    void success() {
        assertEquals(
                TelemetrySimulatorApplication.EXIT_SUCCESS,
                TelemetrySimulatorApplication.run(VALID, ignored -> {}));
    }

    @Test
    @DisplayName("I/O failure returns 2")
    void ioFailure() {
        assertEquals(
                TelemetrySimulatorApplication.EXIT_EXECUTION_FAILURE,
                TelemetrySimulatorApplication.run(
                        VALID,
                        ignored -> {
                            throw new IOException("deliberate");
                        }));
    }

    @Test
    @DisplayName("interruption returns 2 and preserves interrupt status")
    void interruption() {
        try {
            int result =
                    TelemetrySimulatorApplication.run(
                            VALID,
                            ignored -> {
                                throw new InterruptedException("deliberate");
                            });

            assertEquals(TelemetrySimulatorApplication.EXIT_EXECUTION_FAILURE, result);
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }
}
