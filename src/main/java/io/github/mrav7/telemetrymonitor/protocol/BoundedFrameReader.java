package io.github.mrav7.telemetrymonitor.protocol;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Objects;

/**
 * Splits a byte stream into newline-delimited frames without ever buffering more than the
 * configured maximum.
 *
 * <p>TCP delivers bytes, not messages, so this reader never assumes that one read returns one
 * frame. It accumulates bytes until {@code LF} and enforces the size limit <em>while</em>
 * accumulating, so an oversized frame is detected without a String or an unbounded buffer ever
 * being created.
 *
 * <p>Byte-count semantics:
 *
 * <ul>
 *   <li>the {@code LF} delimiter does not count toward the limit;
 *   <li>a {@code CR} immediately before {@code LF} does count, and is removed only after the limit
 *       has been satisfied.
 * </ul>
 *
 * <p>Reads are performed one byte at a time against the supplied stream. The networking layer is
 * expected to hand in a buffered stream, which keeps this class free of its own read-buffer
 * bookkeeping and keeps fragmentation behaviour easy to test.
 *
 * <p>Not thread-safe: one reader belongs to one stream.
 */
public final class BoundedFrameReader {

    private static final int LF = '\n';
    private static final byte CR = '\r';
    private static final int INITIAL_CAPACITY = 256;

    private final InputStream source;
    private final int maxMessageBytes;

    private byte[] buffer;
    private int length;

    /**
     * @param source stream to frame; not closed by this reader
     * @param maxMessageBytes maximum bytes per frame excluding the delimiter, must be positive
     */
    public BoundedFrameReader(InputStream source, int maxMessageBytes) {
        this.source = Objects.requireNonNull(source, "source");
        if (maxMessageBytes <= 0) {
            throw new IllegalArgumentException(
                    "maxMessageBytes must be positive, but was " + maxMessageBytes);
        }
        this.maxMessageBytes = maxMessageBytes;
        this.buffer = new byte[Math.min(INITIAL_CAPACITY, maxMessageBytes)];
    }

    public int maxMessageBytes() {
        return maxMessageBytes;
    }

    /** Reads the next frame, or reports rejection or end of stream. */
    public FrameReadResult readFrame() throws IOException {
        length = 0;
        while (true) {
            int read = source.read();

            if (read < 0) {
                boolean discarded = length > 0;
                length = 0;
                return new FrameReadResult.EndOfStream(discarded);
            }

            if (read == LF) {
                return completeFrame();
            }

            if (length == maxMessageBytes) {
                // This is byte maxMessageBytes + 1 with no delimiter in sight. Stop here: the
                // payload is not accumulated further and the stream is not drained, because the
                // connection cannot be realigned to a frame boundary.
                length = 0;
                return new FrameReadResult.Rejected(RejectionReason.MESSAGE_TOO_LARGE);
            }

            append((byte) read);
        }
    }

    private FrameReadResult completeFrame() {
        int payloadLength = length;
        if (payloadLength > 0 && buffer[payloadLength - 1] == CR) {
            payloadLength--;
        }
        if (payloadLength == 0) {
            return new FrameReadResult.Rejected(RejectionReason.EMPTY_FRAME);
        }
        return new FrameReadResult.Frame(Arrays.copyOf(buffer, payloadLength));
    }

    private void append(byte value) {
        if (length == buffer.length) {
            grow();
        }
        buffer[length++] = value;
    }

    private void grow() {
        // Never grows past the configured maximum, so the arithmetic cannot overflow.
        int capacity =
                buffer.length > maxMessageBytes / 2 ? maxMessageBytes : buffer.length * 2;
        buffer = Arrays.copyOf(buffer, capacity);
    }
}
