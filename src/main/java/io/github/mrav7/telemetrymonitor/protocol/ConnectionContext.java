package io.github.mrav7.telemetrymonitor.protocol;

import java.util.Objects;

/**
 * Identifies the connection a telemetry message arrived on.
 *
 * <p>This is diagnostic context, not identity. A connection may carry events for several
 * {@code sourceId} values, and the same source may reconnect, so nothing here may be used to decide
 * which source an event belongs to.
 *
 * @param connectionId monitor-assigned number, unique for the lifetime of the process
 * @param remote       remote socket address as text, recorded without any name lookup
 */
public record ConnectionContext(long connectionId, String remote) {

    public ConnectionContext {
        Objects.requireNonNull(remote, "remote");
    }

    @Override
    public String toString() {
        return "connection_id=" + connectionId + " remote=" + remote;
    }
}
