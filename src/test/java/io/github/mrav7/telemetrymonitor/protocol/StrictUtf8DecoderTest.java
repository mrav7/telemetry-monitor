package io.github.mrav7.telemetrymonitor.protocol;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Covers strict UTF-8 handling: malformed bytes are reported rather than replaced, and a rejected
 * frame does not prevent the next frame from being read.
 */
class StrictUtf8DecoderTest {

    private static RejectionReason reasonOf(byte[] bytes) {
        return assertThrows(MessageRejectedException.class, () -> StrictUtf8Decoder.decode(bytes))
                .reason();
    }

    @Test
    @DisplayName("ASCII decodes unchanged")
    void decodesAscii() throws Exception {
        assertEquals("source-01", StrictUtf8Decoder.decode("source-01".getBytes(UTF_8)));
    }

    @Test
    @DisplayName("multi-byte sequences decode correctly")
    void decodesMultiByteSequences() throws Exception {
        String text = "mínima-é中🚀";

        assertEquals(text, StrictUtf8Decoder.decode(text.getBytes(UTF_8)));
    }

    @Test
    @DisplayName("an invalid leading byte is rejected")
    void rejectsInvalidLeadingByte() {
        assertEquals(RejectionReason.INVALID_UTF8, reasonOf(new byte[] {(byte) 0xFF, 'a'}));
    }

    @Test
    @DisplayName("an invalid continuation byte is rejected")
    void rejectsInvalidContinuationByte() {
        // 0xC3 starts a two-byte sequence; 0x28 is not a valid continuation.
        assertEquals(RejectionReason.INVALID_UTF8, reasonOf(new byte[] {(byte) 0xC3, 0x28}));
    }

    @Test
    @DisplayName("a truncated multi-byte sequence inside a complete frame is rejected")
    void rejectsTruncatedSequence() {
        byte[] complete = "é".getBytes(UTF_8);
        assertEquals(2, complete.length);

        // The frame was delimited, but its last character is missing its continuation byte.
        assertEquals(RejectionReason.INVALID_UTF8, reasonOf(new byte[] {complete[0]}));
    }

    @Test
    @DisplayName("invalid bytes are not silently replaced")
    void doesNotSubstituteReplacementCharacters() {
        byte[] malformed = {(byte) 0xC3, 0x28};

        // The lenient JDK conversion would have produced a replacement character instead.
        assertNotEquals("", new String(malformed, UTF_8));
        assertEquals(RejectionReason.INVALID_UTF8, reasonOf(malformed));
    }

    @Test
    @DisplayName("invalid UTF-8 never reaches JSON decoding")
    void invalidUtf8StopsBeforeJson() {
        TelemetryMessageDecoder decoder = new TelemetryMessageDecoder();
        byte[] frame = {'{', (byte) 0xFF, '}'};

        assertEquals(
                RejectionReason.INVALID_UTF8,
                assertThrows(MessageRejectedException.class, () -> decoder.decode(frame)).reason());
    }

    @Test
    @DisplayName("a frame rejected for invalid UTF-8 leaves framing able to continue")
    void framingContinuesAfterInvalidUtf8() throws IOException {
        byte[] input = {(byte) 0xC3, 0x28, '\n', 'o', 'k', '\n'};
        BoundedFrameReader reader = new BoundedFrameReader(new ByteArrayInputStream(input), 64);

        FrameReadResult first = reader.readFrame();
        byte[] firstPayload = ((FrameReadResult.Frame) first).payload();
        assertEquals(RejectionReason.INVALID_UTF8, reasonOf(firstPayload));

        FrameReadResult second = reader.readFrame();
        assertEquals("ok", new String(((FrameReadResult.Frame) second).payload(), UTF_8));
    }
}
