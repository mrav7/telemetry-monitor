package io.github.mrav7.telemetrymonitor.protocol;

import java.util.Objects;

/**
 * Outcome of one attempt to read a frame from a byte stream.
 *
 * <p>Reaching the end of the stream is an ordinary outcome rather than a failure, so the three
 * cases are modelled as values instead of exceptions.
 */
public sealed interface FrameReadResult {

    /**
     * A complete frame, delimited and within the byte limit, with any trailing CR removed.
     *
     * <p>The array is freshly allocated per frame and not retained by the reader, so the caller may
     * use it without copying.
     */
    record Frame(byte[] payload) implements FrameReadResult {
        public Frame {
            Objects.requireNonNull(payload, "payload");
        }

        public int length() {
            return payload.length;
        }
    }

    /**
     * The frame was delimited but unusable, or the byte limit was exceeded.
     *
     * <p>Consult {@link RejectionReason#terminatesConnection()} to decide whether reading may
     * continue.
     */
    record Rejected(RejectionReason reason) implements FrameReadResult {
        public Rejected {
            Objects.requireNonNull(reason, "reason");
        }
    }

    /**
     * The stream ended.
     *
     * @param incompleteFrameDiscarded whether trailing bytes without a delimiter were dropped;
     *     those bytes are never decoded
     */
    record EndOfStream(boolean incompleteFrameDiscarded) implements FrameReadResult {}
}
