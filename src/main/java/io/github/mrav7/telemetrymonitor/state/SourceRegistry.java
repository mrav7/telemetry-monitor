package io.github.mrav7.telemetrymonitor.state;

import io.github.mrav7.telemetrymonitor.protocol.TelemetryEnvelope;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The single authority for operational source state.
 *
 * <p>Only processing workers reach this class, through {@link #apply(TelemetryEnvelope)}. Connection
 * tasks never touch source state: they read, frame, decode, validate and enqueue, which keeps the
 * decision of what a source's state is in exactly one place.
 *
 * <p><b>Per-source atomicity.</b> Every state change runs inside {@link ConcurrentHashMap#compute}
 * for the source's key, so two workers processing the same source cannot lose each other's update,
 * and a stale evaluation sees the state as it is at the moment it decides rather than a copy read
 * earlier. Snapshots are immutable, so a reader never observes a partially applied change.
 *
 * <p><b>Global capacity.</b> {@code TM_MAX_SOURCES} is a bound across all keys, which per-key
 * atomicity alone cannot enforce. A separate counter is claimed with a compare-and-set before a new
 * source is stored, inside the per-key computation: the claim can only succeed below the limit, and
 * it is only made on the path that immediately stores the source, so the count can neither overshoot
 * the limit nor leak a slot. Updates to sources that already exist take no claim and are therefore
 * never serialised against each other.
 *
 * <p>The registry never removes a source. A stale source keeps its slot for the life of the process;
 * there is no eviction, and state is in memory only.
 */
public final class SourceRegistry {

    private static final Logger log = LoggerFactory.getLogger(SourceRegistry.class);

    private final ConcurrentHashMap<String, SourceSnapshot> sources = new ConcurrentHashMap<>();

    /**
     * Number of sources admitted. Never decreases, because sources are never removed, so it is
     * exactly the map size — but it is the value that is claimed before a source is stored, which is
     * what keeps simultaneous admissions from exceeding the limit.
     */
    private final AtomicInteger admitted = new AtomicInteger();

    private final int maxSources;

    /** @param maxSources upper bound on distinct known sources; {@code TM_MAX_SOURCES} */
    public SourceRegistry(int maxSources) {
        if (maxSources <= 0) {
            throw new IllegalArgumentException("maxSources must be positive, but was " + maxSources);
        }
        this.maxSources = maxSources;
    }

    /**
     * Applies one validated, enqueued event to the state of its source.
     *
     * <p>Called on a processing worker. A source that is unknown and cannot be admitted is reported
     * and skipped: that is an expected operational outcome, not a failure, so the worker, the
     * pipeline and the connection all carry on, and known sources keep being updated.
     *
     * @return what the event did to the registry
     */
    public SourceUpdate apply(TelemetryEnvelope envelope) {
        Objects.requireNonNull(envelope, "envelope");
        String sourceId = envelope.event().sourceId();
        Instant receivedAt = envelope.receivedAt();

        // Written only by the computation below, which runs while this thread holds the bin lock
        // for this key, and read only after it returns.
        SourceUpdate[] outcome = {SourceUpdate.REJECTED_AT_CAPACITY};

        // compute returns the value this invocation produced, while it still holds the bin lock,
        // so it is the snapshot this apply created rather than whatever a later lookup would find.
        // It is null exactly when the source was refused, which is the case report() expects.
        SourceSnapshot applied =
                sources.compute(
                        sourceId,
                        (id, current) -> {
                            if (current != null) {
                                outcome[0] =
                                        current.status() == SourceStatus.STALE
                                                ? SourceUpdate.RECOVERED
                                                : SourceUpdate.UPDATED;
                                return current.accept(receivedAt);
                            }
                            SourceSnapshot created = SourceSnapshot.firstAccepted(id, receivedAt);
                            if (!claimSlot()) {
                                // Leaving the key absent is the rejection: nothing is created, and
                                // no existing source is disturbed.
                                return null;
                            }
                            outcome[0] = SourceUpdate.ADMITTED;
                            return created;
                        });

        report(outcome[0], sourceId, applied);
        return outcome[0];
    }

    /**
     * Marks every source that has been silent for at least {@code staleAfter} as of {@code now}.
     *
     * <p>Called by the scheduled monitor, never by a worker or the accept loop. The silence test is
     * re-evaluated against the current snapshot inside the atomic update, so telemetry accepted
     * between reading a source and updating it cannot be overwritten by this older view. Only
     * {@code ONLINE → STALE} is performed here; returning to {@code ONLINE} is the job of accepted
     * telemetry alone.
     *
     * @return how many sources changed to {@link SourceStatus#STALE} during this pass
     */
    public int markStaleSources(Instant now, Duration staleAfter) {
        Objects.requireNonNull(now, "now");
        Objects.requireNonNull(staleAfter, "staleAfter");

        int transitions = 0;
        SourceSnapshot[] transitioned = new SourceSnapshot[1];
        for (Map.Entry<String, SourceSnapshot> entry : sources.entrySet()) {
            if (entry.getValue().status() != SourceStatus.ONLINE) {
                // Cheap skip on a possibly outdated read; the decision below is made on the
                // current snapshot, so this can only save work, never change the outcome.
                continue;
            }
            transitioned[0] = null;
            sources.computeIfPresent(
                    entry.getKey(),
                    (id, current) -> {
                        if (current.status() != SourceStatus.ONLINE
                                || !current.silentFor(now, staleAfter)) {
                            return current;
                        }
                        SourceSnapshot stale = current.markStale();
                        transitioned[0] = stale;
                        return stale;
                    });
            if (transitioned[0] != null) {
                transitions++;
                log.info(
                        "event=source_stale source_id={} last_accepted_at={} silent_seconds={}",
                        transitioned[0].sourceId(),
                        transitioned[0].lastAcceptedAt(),
                        Duration.between(transitioned[0].lastAcceptedAt(), now).toSeconds());
            }
        }
        return transitions;
    }

    /** The current state of one source, or empty when the monitor has never accepted it. */
    public Optional<SourceSnapshot> snapshot(String sourceId) {
        return Optional.ofNullable(sources.get(Objects.requireNonNull(sourceId, "sourceId")));
    }

    /** The state of every known source, as it stood during the call. */
    public Collection<SourceSnapshot> snapshots() {
        return List.copyOf(sources.values());
    }

    /** How many sources are known. Never exceeds {@code TM_MAX_SOURCES}. */
    public int knownSourceCount() {
        return sources.size();
    }

    public int maxSources() {
        return maxSources;
    }

    /**
     * Claims one source slot, or reports that the registry is full.
     *
     * <p>The compare-and-set is what makes simultaneous admissions safe: each caller only succeeds
     * against the value it read, so the count passes through every value up to the limit and stops.
     */
    private boolean claimSlot() {
        while (true) {
            int current = admitted.get();
            if (current >= maxSources) {
                return false;
            }
            if (admitted.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    /**
     * Reports one outcome, describing it with the snapshot that outcome produced.
     *
     * <p>The timestamps come from {@code applied}, never from the incoming event and never from a
     * fresh lookup. An event applied out of order keeps the later reception time the source already
     * had, so reporting the incoming one would claim a source is less fresh than the transition
     * actually left it; and a lookup made after the update could describe a state some other
     * worker has since produced, rather than this transition.
     *
     * @param applied the snapshot this apply produced, or {@code null} when the source was refused
     */
    private void report(SourceUpdate outcome, String sourceId, SourceSnapshot applied) {
        switch (outcome) {
            case ADMITTED ->
                    log.info(
                            "event=source_first_seen source_id={} first_accepted_at={} known_sources={}",
                            sourceId,
                            applied.firstAcceptedAt(),
                            sources.size());
            case RECOVERED ->
                    log.info(
                            "event=source_online source_id={} last_accepted_at={}",
                            sourceId,
                            applied.lastAcceptedAt());
            case REJECTED_AT_CAPACITY ->
                    log.warn(
                            "event=source_rejected source_id={} reason=max_sources limit={}",
                            sourceId,
                            maxSources);
            case UPDATED -> {
                // The normal path. Logging every accepted event would drown the log under load.
            }
        }
    }
}
