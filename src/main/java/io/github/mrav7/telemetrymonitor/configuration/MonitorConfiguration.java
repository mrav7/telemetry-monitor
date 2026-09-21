package io.github.mrav7.telemetrymonitor.configuration;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Objects;

/**
 * Validated, immutable operational configuration.
 *
 * <p>An instance only exists if every value passed {@link ConfigurationLoader} validation, so the
 * rest of the service can use these values without re-checking them.
 *
 * @param bindAddress        address the TCP listener will bind to
 * @param port               TCP port, 1..65535
 * @param maxConnections     upper bound on simultaneously active connections
 * @param workerThreads      fixed number of processing workers
 * @param queueCapacity      capacity of the bounded processing queue
 * @param maxMessageBytes    maximum bytes in one frame, excluding the LF delimiter
 * @param maxSources         upper bound on distinct sources held in the registry
 * @param staleAfter         silence after which a source is considered stale
 * @param staleCheckInterval how often staleness is evaluated
 * @param shutdownGrace      global deadline for the whole shutdown sequence
 * @param logLevel           effective root log level
 */
public record MonitorConfiguration(
        InetAddress bindAddress,
        int port,
        int maxConnections,
        int workerThreads,
        int queueCapacity,
        int maxMessageBytes,
        int maxSources,
        Duration staleAfter,
        Duration staleCheckInterval,
        Duration shutdownGrace,
        LogLevel logLevel) {

    public MonitorConfiguration {
        Objects.requireNonNull(bindAddress, "bindAddress");
        Objects.requireNonNull(staleAfter, "staleAfter");
        Objects.requireNonNull(staleCheckInterval, "staleCheckInterval");
        Objects.requireNonNull(shutdownGrace, "shutdownGrace");
        Objects.requireNonNull(logLevel, "logLevel");
    }

    /** Single-line, non-sensitive rendering suitable for a startup log record. */
    public String describe() {
        return "bind_address=" + bindAddress.getHostAddress()
                + " port=" + port
                + " max_connections=" + maxConnections
                + " worker_threads=" + workerThreads
                + " queue_capacity=" + queueCapacity
                + " max_message_bytes=" + maxMessageBytes
                + " max_sources=" + maxSources
                + " stale_after_seconds=" + staleAfter.toSeconds()
                + " stale_check_interval_seconds=" + staleCheckInterval.toSeconds()
                + " shutdown_grace_seconds=" + shutdownGrace.toSeconds()
                + " log_level=" + logLevel;
    }
}
