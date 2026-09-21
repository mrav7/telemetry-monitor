package io.github.mrav7.telemetrymonitor.protocol;

/**
 * Stable reasons a wire message can be rejected.
 *
 * <p>These exist so that callers decide what to do from a value, never by matching text in an
 * exception message.
 *
 * <p>{@link #terminatesConnection()} distinguishes the one framing failure that leaves the byte
 * stream unusable from the failures that only discard the current message.
 */
public enum RejectionReason {

    /** Frame contained no bytes once an optional trailing CR was removed. */
    EMPTY_FRAME(false),

    /**
     * The frame exceeded the configured byte limit before a delimiter appeared. The reader stops
     * consuming the payload, so the stream position is no longer aligned to a frame boundary and
     * the connection cannot be recovered.
     */
    MESSAGE_TOO_LARGE(true),

    /** Frame bytes were not valid UTF-8. */
    INVALID_UTF8(false),

    /** Frame text was not a single well-formed JSON object. */
    INVALID_JSON(false),

    /** A required property was absent. */
    MISSING_FIELD(false),

    /** A property the protocol does not define was present. */
    UNEXPECTED_FIELD(false),

    /** A property appeared more than once in the same object. */
    DUPLICATE_FIELD(false),

    /** A property had the wrong JSON type, including an explicit null. */
    INVALID_FIELD_TYPE(false),

    /** The protocol version is not supported by this monitor. */
    UNSUPPORTED_VERSION(false),

    /** {@code sourceId} did not satisfy the identifier contract. */
    INVALID_SOURCE_ID(false),

    /** {@code metric} did not satisfy the identifier contract. */
    INVALID_METRIC(false),

    /** {@code occurredAt} was not a parsable ISO-8601 instant. */
    INVALID_TIMESTAMP(false),

    /** {@code value} was not a finite number. */
    INVALID_VALUE(false);

    private final boolean terminatesConnection;

    RejectionReason(boolean terminatesConnection) {
        this.terminatesConnection = terminatesConnection;
    }

    /**
     * Whether a connection carrying this failure must be closed rather than continued.
     *
     * <p>The networking layer acts on this; nothing here opens or closes sockets.
     */
    public boolean terminatesConnection() {
        return terminatesConnection;
    }
}
