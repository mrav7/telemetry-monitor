package io.github.mrav7.telemetrymonitor.protocol;

import java.time.Instant;
import java.util.Objects;

/**
 * A telemetry message that has passed JSON decoding and semantic validation.
 *
 * <p>Instances are produced by {@link TelemetryMessageDecoder}; invalid wire input never reaches
 * this type. The constructor re-checks the invariants that can be enforced cheaply, so the finite
 * {@code value} guarantee holds structurally rather than only by convention.
 *
 * @param version    protocol version, always 1 for this monitor
 * @param sourceId   identifier declared by the sending source
 * @param occurredAt timestamp declared by the source; never used for liveness
 * @param metric     identifier of the reading
 * @param value      finite measurement
 */
public record TelemetryEvent(
        int version, String sourceId, Instant occurredAt, String metric, double value) {

    public TelemetryEvent {
        Objects.requireNonNull(sourceId, "sourceId");
        Objects.requireNonNull(occurredAt, "occurredAt");
        Objects.requireNonNull(metric, "metric");
        if (!Double.isFinite(value)) {
            throw new IllegalArgumentException("value must be finite, but was " + value);
        }
    }
}
