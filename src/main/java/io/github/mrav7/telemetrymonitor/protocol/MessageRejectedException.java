package io.github.mrav7.telemetrymonitor.protocol;

import java.util.Objects;

/** Signals that a frame was rejected, carrying the machine-readable {@link RejectionReason}. */
public final class MessageRejectedException extends Exception {

    private static final long serialVersionUID = 1L;

    private final transient RejectionReason reason;

    public MessageRejectedException(RejectionReason reason, String detail) {
        super(reason + ": " + detail);
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    public MessageRejectedException(RejectionReason reason, String detail, Throwable cause) {
        super(reason + ": " + detail, cause);
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    public RejectionReason reason() {
        return reason;
    }
}
