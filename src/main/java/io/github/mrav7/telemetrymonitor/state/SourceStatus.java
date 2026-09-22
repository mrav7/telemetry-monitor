package io.github.mrav7.telemetrymonitor.state;

/**
 * Operational state of a known telemetry source.
 *
 * <p>There are exactly two states. A source the monitor has never accepted an event from is not a
 * third state: it is simply absent from the {@link SourceRegistry}.
 */
public enum SourceStatus {

    /** Accepted telemetry arrived recently enough, measured against the configured threshold. */
    ONLINE,

    /**
     * No accepted telemetry for at least the configured threshold. The source stays registered and
     * returns to {@link #ONLINE} as soon as it is accepted again.
     */
    STALE
}
