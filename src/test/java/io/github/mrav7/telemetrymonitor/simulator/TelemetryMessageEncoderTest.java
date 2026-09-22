package io.github.mrav7.telemetrymonitor.simulator;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.mrav7.telemetrymonitor.protocol.MessageRejectedException;
import io.github.mrav7.telemetrymonitor.protocol.RejectionReason;
import io.github.mrav7.telemetrymonitor.protocol.TelemetryEvent;
import io.github.mrav7.telemetrymonitor.protocol.TelemetryMessageDecoder;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Timeout(value = 10, unit = TimeUnit.SECONDS)
class TelemetryMessageEncoderTest {

    private static final Instant OCCURRED_AT = Instant.parse("2026-09-22T12:34:56.789Z");

    private final TelemetryMessageEncoder encoder = new TelemetryMessageEncoder();
    private final TelemetryMessageDecoder decoder = new TelemetryMessageDecoder();

    @Test
    @DisplayName("valid output is one compact UTF-8 protocol-v1 frame")
    void encodesCompatibleWireFrame() throws Exception {
        byte[] frame = encoder.encode("source-01", OCCURRED_AT, "temperature", 18.72);

        assertEquals((byte) '\n', frame[frame.length - 1]);
        assertEquals(1, Arrays.stream(box(frame)).filter(value -> value == (byte) '\n').count());
        String json = new String(withoutLf(frame), UTF_8);
        assertFalse(json.contains("\r"));
        assertFalse(json.contains("\n"));
        assertFalse(json.contains(" "), "valid JSON must not be pretty-printed");
        TelemetryEvent decoded = decoder.decode(withoutLf(frame));
        assertAll(
                () -> assertEquals(1, decoded.version()),
                () -> assertEquals("source-01", decoded.sourceId()),
                () -> assertEquals(OCCURRED_AT, decoded.occurredAt()),
                () -> assertEquals("temperature", decoded.metric()),
                () -> assertEquals(18.72, decoded.value()));
    }

    @ParameterizedTest(name = "{0} is rejected as {1}")
    @MethodSource("malformedCases")
    void malformedFixtureHasClaimedReason(MalformedCase malformedCase, RejectionReason expected)
            throws Exception {
        byte[] frame =
                encoder.encodeMalformed(
                        malformedCase, "source-01", OCCURRED_AT, "temperature", 18.72);

        assertEquals((byte) '\n', frame[frame.length - 1]);
        assertEquals(1, Arrays.stream(box(frame)).filter(value -> value == (byte) '\n').count());
        MessageRejectedException rejection =
                assertThrows(MessageRejectedException.class, () -> decoder.decode(withoutLf(frame)));
        assertEquals(expected, rejection.reason());
    }

    private static Stream<Arguments> malformedCases() {
        return Stream.of(
                Arguments.of(MalformedCase.BROKEN_JSON, RejectionReason.INVALID_JSON),
                Arguments.of(MalformedCase.MISSING_FIELD, RejectionReason.MISSING_FIELD),
                Arguments.of(MalformedCase.UNSUPPORTED_VERSION, RejectionReason.UNSUPPORTED_VERSION),
                Arguments.of(MalformedCase.INVALID_SOURCE_ID, RejectionReason.INVALID_SOURCE_ID),
                Arguments.of(MalformedCase.INVALID_TIMESTAMP, RejectionReason.INVALID_TIMESTAMP),
                Arguments.of(MalformedCase.INVALID_VALUE, RejectionReason.INVALID_VALUE));
    }

    private static byte[] withoutLf(byte[] frame) {
        assertTrue(frame.length > 1);
        return Arrays.copyOf(frame, frame.length - 1);
    }

    private static Byte[] box(byte[] bytes) {
        Byte[] boxed = new Byte[bytes.length];
        for (int index = 0; index < bytes.length; index++) {
            boxed[index] = bytes[index];
        }
        return boxed;
    }
}
