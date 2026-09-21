package io.github.mrav7.telemetrymonitor;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Guards the project baseline: the build targets the expected Java release and the entry point is
 * callable. These checks are what make a green build meaningful at this stage.
 */
class TelemetryMonitorApplicationTest {

    private static final int EXPECTED_JAVA_FEATURE_RELEASE = 25;

    @Test
    @DisplayName("tests execute on the Java release the project targets")
    void runsOnExpectedJavaRelease() {
        assertEquals(
                EXPECTED_JAVA_FEATURE_RELEASE,
                Runtime.version().feature(),
                "tests must run on the Java release the project is compiled against");
    }

    @Test
    @DisplayName("application entry point completes without failing")
    void entryPointCompletes() {
        assertDoesNotThrow(() -> TelemetryMonitorApplication.main(new String[0]));
    }
}
