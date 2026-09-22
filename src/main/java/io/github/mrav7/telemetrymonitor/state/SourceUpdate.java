package io.github.mrav7.telemetrymonitor.state;

/**
 * What applying one validated, enqueued event did at the source-state boundary.
 *
 * <p>The event is not yet an accepted event when it reaches this boundary: source admission is one
 * of the conditions for acceptance, and it is still unresolved. An event that ends in
 * {@link #REJECTED_AT_CAPACITY} was validated and enqueued but never accepted, and so counts
 * towards nothing.
 */
public enum SourceUpdate {

    /** The source was unknown and capacity was available, so it was created {@code ONLINE}. */
    ADMITTED,

    /** A known online source was updated by the accepted event. */
    UPDATED,

    /** A known stale source was updated by the accepted event and is online again. */
    RECOVERED,

    /**
     * The source was unknown and the registry was already holding {@code TM_MAX_SOURCES} sources.
     * Nothing was created and no existing source was touched or removed.
     */
    REJECTED_AT_CAPACITY;

    /** Whether the event was accepted and reached source state. */
    public boolean applied() {
        return this != REJECTED_AT_CAPACITY;
    }
}
