package io.github.mrav7.telemetrymonitor.protocol;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Covers the framing contract: delimiters, the exact byte boundary, end-of-stream handling, and the
 * rule that the size limit is enforced in bytes while reading rather than on a finished string.
 */
class BoundedFrameReaderTest {

    private static final int DEFAULT_MAX = 8192;

    private static BoundedFrameReader reader(String input, int maxMessageBytes) {
        return new BoundedFrameReader(
                new ByteArrayInputStream(input.getBytes(UTF_8)), maxMessageBytes);
    }

    private static BoundedFrameReader reader(byte[] input, int maxMessageBytes) {
        return new BoundedFrameReader(new ByteArrayInputStream(input), maxMessageBytes);
    }

    private static String frameText(FrameReadResult result) {
        return new String(assertInstanceOf(FrameReadResult.Frame.class, result).payload(), UTF_8);
    }

    private static RejectionReason reasonOf(FrameReadResult result) {
        return assertInstanceOf(FrameReadResult.Rejected.class, result).reason();
    }

    @Nested
    @DisplayName("delimiters")
    class Delimiters {

        @Test
        @DisplayName("an LF-delimited frame is returned without the delimiter")
        void readsLfDelimitedFrame() throws IOException {
            assertEquals("hello", frameText(reader("hello\n", DEFAULT_MAX).readFrame()));
        }

        @Test
        @DisplayName("a CRLF-delimited frame has the CR stripped")
        void readsCrlfDelimitedFrame() throws IOException {
            assertEquals("hello", frameText(reader("hello\r\n", DEFAULT_MAX).readFrame()));
        }

        @Test
        @DisplayName("a CR that is not immediately before LF is kept as payload")
        void keepsInteriorCarriageReturn() throws IOException {
            assertEquals("a\rb", frameText(reader("a\rb\n", DEFAULT_MAX).readFrame()));
        }

        @Test
        @DisplayName("frames arrive in order across one stream")
        void readsSequentialFrames() throws IOException {
            BoundedFrameReader reader = reader("one\ntwo\r\nthree\n", DEFAULT_MAX);

            assertEquals("one", frameText(reader.readFrame()));
            assertEquals("two", frameText(reader.readFrame()));
            assertEquals("three", frameText(reader.readFrame()));
            assertInstanceOf(FrameReadResult.EndOfStream.class, reader.readFrame());
        }
    }

    @Nested
    @DisplayName("fragmented input")
    class FragmentedInput {

        @ParameterizedTest(name = "frames survive {0}-byte reads")
        @ValueSource(ints = {1, 2, 3, 5, 64})
        void reassemblesFramesAcrossReadBoundaries(int chunkSize) throws IOException {
            InputStream source =
                    new ChunkedInputStream("alpha\nbravo\r\ncharlie\n".getBytes(UTF_8), chunkSize);
            BoundedFrameReader reader = new BoundedFrameReader(source, DEFAULT_MAX);

            assertEquals("alpha", frameText(reader.readFrame()));
            assertEquals("bravo", frameText(reader.readFrame()));
            assertEquals("charlie", frameText(reader.readFrame()));
        }

        @Test
        @DisplayName("a CR and its LF may arrive in separate reads")
        void handlesCarriageReturnSplitFromLineFeed() throws IOException {
            InputStream source = new ChunkedInputStream("payload\r\n".getBytes(UTF_8), 1);

            assertEquals(
                    "payload",
                    frameText(new BoundedFrameReader(source, DEFAULT_MAX).readFrame()));
        }

        @Test
        @DisplayName("a multi-byte character split across reads is only decoded once framed")
        void handlesMultiByteCharacterSplitAcrossReads() throws IOException {
            byte[] input = "temperatura-mínima\n".getBytes(UTF_8);
            InputStream source = new ChunkedInputStream(input, 1);

            FrameReadResult result = new BoundedFrameReader(source, DEFAULT_MAX).readFrame();

            assertEquals("temperatura-mínima", frameText(result));
        }
    }

    @Nested
    @DisplayName("empty frames")
    class EmptyFrames {

        @Test
        @DisplayName("a bare LF is rejected as an empty frame")
        void rejectsBareLineFeed() throws IOException {
            assertEquals(RejectionReason.EMPTY_FRAME, reasonOf(reader("\n", DEFAULT_MAX).readFrame()));
        }

        @Test
        @DisplayName("a bare CRLF is rejected as an empty frame")
        void rejectsBareCarriageReturnLineFeed() throws IOException {
            assertEquals(
                    RejectionReason.EMPTY_FRAME, reasonOf(reader("\r\n", DEFAULT_MAX).readFrame()));
        }

        @Test
        @DisplayName("an empty frame does not stop the next frame from being read")
        void recoversAfterEmptyFrame() throws IOException {
            BoundedFrameReader reader = reader("\n\r\nusable\n", DEFAULT_MAX);

            assertEquals(RejectionReason.EMPTY_FRAME, reasonOf(reader.readFrame()));
            assertEquals(RejectionReason.EMPTY_FRAME, reasonOf(reader.readFrame()));
            assertEquals("usable", frameText(reader.readFrame()));
        }

        @Test
        @DisplayName("an empty frame is not end of stream")
        void emptyFrameIsNotEndOfStream() throws IOException {
            assertFalse(reader("\n", DEFAULT_MAX).readFrame() instanceof FrameReadResult.EndOfStream);
        }
    }

    @Nested
    @DisplayName("end of stream")
    class EndOfStream {

        @Test
        @DisplayName("EOF with no pending bytes is a clean end")
        void cleanEndOfStream() throws IOException {
            FrameReadResult result = reader("", DEFAULT_MAX).readFrame();

            assertFalse(
                    assertInstanceOf(FrameReadResult.EndOfStream.class, result)
                            .incompleteFrameDiscarded());
        }

        @Test
        @DisplayName("a complete frame followed by EOF ends cleanly")
        void frameThenCleanEndOfStream() throws IOException {
            BoundedFrameReader reader = reader("done\n", DEFAULT_MAX);

            assertEquals("done", frameText(reader.readFrame()));
            assertFalse(
                    assertInstanceOf(FrameReadResult.EndOfStream.class, reader.readFrame())
                            .incompleteFrameDiscarded());
        }

        @Test
        @DisplayName("EOF with undelimited bytes discards them and reports the discard")
        void incompleteFrameAtEndOfStreamIsDiscarded() throws IOException {
            FrameReadResult result = reader("{\"partial\":tru", DEFAULT_MAX).readFrame();

            assertTrue(
                    assertInstanceOf(FrameReadResult.EndOfStream.class, result)
                            .incompleteFrameDiscarded());
        }

        @Test
        @DisplayName("an unterminated trailing frame never reaches the caller as a frame")
        void incompleteTrailingFrameIsNeverReturnedAsAFrame() throws IOException {
            BoundedFrameReader reader = reader("complete\nincomplete", DEFAULT_MAX);

            assertEquals("complete", frameText(reader.readFrame()));
            FrameReadResult second = reader.readFrame();
            assertFalse(
                    second instanceof FrameReadResult.Frame,
                    "undelimited trailing bytes must not be surfaced as a frame");
            assertTrue(
                    assertInstanceOf(FrameReadResult.EndOfStream.class, second)
                            .incompleteFrameDiscarded());
        }
    }

    @Nested
    @DisplayName("size limit")
    class SizeLimit {

        @Test
        @DisplayName("a frame of exactly the maximum is accepted")
        void acceptsExactlyMaximumBytes() throws IOException {
            String payload = "x".repeat(16);

            assertEquals(payload, frameText(reader(payload + "\n", 16).readFrame()));
        }

        @Test
        @DisplayName("one byte over the maximum is rejected")
        void rejectsOneByteOverMaximum() throws IOException {
            assertEquals(
                    RejectionReason.MESSAGE_TOO_LARGE,
                    reasonOf(reader("x".repeat(17) + "\n", 16).readFrame()));
        }

        @Test
        @DisplayName("the CR of a CRLF counts toward the limit and is stripped afterwards")
        void carriageReturnCountsTowardTheLimit() throws IOException {
            // 15 payload bytes + CR == 16 bytes before LF, exactly at the limit.
            String payload = "x".repeat(15);

            assertEquals(payload, frameText(reader(payload + "\r\n", 16).readFrame()));
        }

        @Test
        @DisplayName("a maximum-length payload plus CR exceeds the limit")
        void maximumPayloadPlusCarriageReturnIsOversized() throws IOException {
            // 16 payload bytes + CR == 17 bytes before LF.
            assertEquals(
                    RejectionReason.MESSAGE_TOO_LARGE,
                    reasonOf(reader("x".repeat(16) + "\r\n", 16).readFrame()));
        }

        @Test
        @DisplayName("the LF delimiter itself does not count toward the limit")
        void delimiterDoesNotCount() throws IOException {
            assertEquals("abcd", frameText(reader("abcd\n", 4).readFrame()));
        }

        @Test
        @DisplayName("oversize is detected before any delimiter arrives")
        void detectsOversizeWithoutADelimiter() throws IOException {
            ChunkedInputStream source = new ChunkedInputStream("x".repeat(1000).getBytes(UTF_8), 8);

            assertEquals(
                    RejectionReason.MESSAGE_TOO_LARGE,
                    reasonOf(new BoundedFrameReader(source, 16).readFrame()));
        }

        @Test
        @DisplayName("oversize stops immediately instead of draining to the next delimiter")
        void doesNotDrainAfterOversize() throws IOException {
            byte[] input = ("x".repeat(100) + "\nnext\n").getBytes(UTF_8);
            ChunkedInputStream source = new ChunkedInputStream(input, 1);
            BoundedFrameReader reader = new BoundedFrameReader(source, 16);

            assertEquals(RejectionReason.MESSAGE_TOO_LARGE, reasonOf(reader.readFrame()));
            // Only the first 17 bytes were consumed: the limit plus the byte that broke it.
            assertEquals(input.length - 17, source.remaining());
        }

        @Test
        @DisplayName("oversize is terminal for a connection, other rejections are not")
        void oversizeIsTheOnlyTerminalFramingFailure() {
            assertTrue(RejectionReason.MESSAGE_TOO_LARGE.terminatesConnection());
            assertFalse(RejectionReason.EMPTY_FRAME.terminatesConnection());
        }

        @Test
        @DisplayName("the limit counts bytes, not Java characters")
        void limitIsMeasuredInBytesNotCharacters() throws IOException {
            // Three accented characters are three Java chars but six UTF-8 bytes.
            byte[] threeChars = "ééé\n".getBytes(UTF_8);
            assertEquals(7, threeChars.length);

            assertEquals(
                    RejectionReason.MESSAGE_TOO_LARGE, reasonOf(reader(threeChars, 4).readFrame()));

            // Two of the same characters are exactly four bytes, so they fit.
            assertEquals("éé", frameText(reader("éé\n".getBytes(UTF_8), 4).readFrame()));
        }

        @Test
        @DisplayName("a frame at the limit is returned byte-for-byte")
        void returnsExactPayloadBytes() throws IOException {
            byte[] payload = "éé".getBytes(UTF_8);
            FrameReadResult result = reader("éé\n".getBytes(UTF_8), 4).readFrame();

            assertArrayEquals(
                    payload, assertInstanceOf(FrameReadResult.Frame.class, result).payload());
        }
    }

    @Test
    @DisplayName("a non-positive maximum is rejected at construction")
    void rejectsNonPositiveMaximum() {
        org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new BoundedFrameReader(new ByteArrayInputStream(new byte[0]), 0));
    }

    @Test
    @DisplayName("frames larger than the initial buffer still assemble correctly")
    void growsBufferUpToTheConfiguredMaximum() throws IOException {
        String payload = "y".repeat(5000);

        assertEquals(payload, frameText(reader(payload + "\n", DEFAULT_MAX).readFrame()));
    }
}
